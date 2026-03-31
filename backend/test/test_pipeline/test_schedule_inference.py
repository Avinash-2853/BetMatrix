
import unittest
from unittest.mock import MagicMock, patch, mock_open, call, ANY
import sys
import os
import signal
import pandas as pd
import logging
import importlib
import time
import types
from contextlib import ExitStack

# Helper to mock missing modules
def mock_module(module_name):
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    return mod


# Helper to import the module with mocked side effects and return mocks
def import_schedule_inference(extra_patches=None, capture_mocks=None, setup_hook=None):
    # Remove from sys.modules to force re-execution
    if 'app.pipeline.schedule_inference' in sys.modules:
        del sys.modules['app.pipeline.schedule_inference']
    
    # Mock apscheduler and its submodules
    mock_apscheduler = MagicMock()
    sys.modules['apscheduler'] = mock_apscheduler
    sys.modules['apscheduler.schedulers'] = MagicMock()
    # Create distinct mocks for submodules so we can configure them
    mock_background = MagicMock()
    sys.modules['apscheduler.schedulers.background'] = mock_background
    sys.modules['apscheduler.triggers'] = MagicMock()
    sys.modules['apscheduler.triggers.interval'] = MagicMock()
    sys.modules['apscheduler.triggers.cron'] = MagicMock()
    sys.modules['apscheduler.triggers.date'] = MagicMock()
    sys.modules['apscheduler.jobstores'] = MagicMock()
    sys.modules['apscheduler.jobstores.memory'] = MagicMock()
    
    # Allow custom setup before import (e.g. configuring module members)
    if setup_hook:
        setup_hook(sys.modules)
    
    stack = ExitStack()
    mocks = {}
    
    # Define default patches
    patches = {
        'open': patch('builtins.open', mock_open(read_data="123")),
        'makedirs': patch('os.makedirs'),
        'exists': patch('os.path.exists', return_value=False),
        'kill': patch('os.kill'),
        'remove': patch('os.remove'),
        'getpid': patch('os.getpid', return_value=12345),
        'basicConfig': patch('logging.basicConfig'),
        'FileHandler': patch('logging.FileHandler'),
        'StreamHandler': patch('logging.StreamHandler'),
        'getLogger': patch('logging.getLogger'),
        'signal': patch('signal.signal'),
        'atexit': patch('atexit.register'),
        'load_dotenv': patch('dotenv.load_dotenv'),
        'exit': patch('sys.exit')
    }
    
    if extra_patches:
        patches.update(extra_patches)
        
    for name, p in patches.items():
        mocks[name] = stack.enter_context(p)
    
    if capture_mocks is not None:
        capture_mocks.update(mocks)
        
    try:
        import app.pipeline.schedule_inference as module
        return module, mocks, stack.close
    except:
        stack.close()
        raise

class TestScheduleInference(unittest.TestCase):
    def setUp(self):
        self.env_patcher = patch.dict(os.environ, {
            "SKIP_SCHEDULER_LOOP": "1",
            "scheduler_inference_log_path": "logs/test.log",
            "data_input_path": "data/test",
            "models_path": "models/test"
        })
        self.env_patcher.start()
        self.cleanup_funcs = []

    def tearDown(self):
        for func in self.cleanup_funcs:
            func()
        self.env_patcher.stop()
        if 'app.pipeline.schedule_inference' in sys.modules:
            del sys.modules['app.pipeline.schedule_inference']

    def load_module(self, extra_patches=None, capture_mocks=None, setup_hook=None):
        module, mocks, cleanup = import_schedule_inference(extra_patches, capture_mocks, setup_hook)
        self.cleanup_funcs.append(cleanup)
        return module, mocks

    # ... (Tests) ...

    def test_startup_failure(self):
         # Make scheduler.start() raise Exception
         mock_scheduler_instance = MagicMock()
         mock_scheduler_instance.start.side_effect = Exception("Startup Fail")
         mock_bg_scheduler = MagicMock(return_value=mock_scheduler_instance)
         
         # Direct setup via hook to ensure import uses OUR mock class
         def configure_scheduler(modules):
             modules['apscheduler.schedulers.background'].BackgroundScheduler = mock_bg_scheduler
         
         # We still need to patch sys.exit to verify it was called
         extra = {
             'exit': patch('sys.exit')
         }
         
         mocks_holder = {}
         
         try:
             self.load_module(extra, capture_mocks=mocks_holder, setup_hook=configure_scheduler)
         except SystemExit:  # NOSONAR
              pass
         
         mocks_holder['getLogger'].return_value.exception.assert_called()
         mocks_holder['exit'].assert_called_with(1)

    def test_initialization_no_env_vars(self):
        # Test default paths when env vars are missing
        # We need SKIP_SCHEDULER_LOOP to be 1, but others empty
        envs = {"SKIP_SCHEDULER_LOOP": "1"}
        with patch.dict(os.environ, envs, clear=True):
             _, mocks = self.load_module()
             self.assertTrue(mocks['makedirs'].called)

    def test_initialization_creates_directories_and_logger(self):
        _, mocks = self.load_module()
        
        # Verify directories created
        self.assertTrue(mocks['makedirs'].called)
        # Verify logging setup
        mocks['FileHandler'].assert_called()
        mocks['basicConfig'].assert_called()

    def test_scheduler_setup(self):
        module, _ = self.load_module()
        
        # Verify add_job was called twice
        self.assertEqual(module.scheduler.add_job.call_count, 2)
        # Verify start was called
        module.scheduler.start.assert_called_once()

    def test_lock_file_creation(self):
        _, mocks = self.load_module()
        mocks['open']().write.assert_called_with('12345')

    def test_existing_lock_file_process_running(self):
        extra = {
            'exists': patch('os.path.exists', return_value=True),
            'open': patch('builtins.open', mock_open(read_data="999"))
        }
        _, mocks = self.load_module(extra)
        
        mocks['kill'].assert_called_with(999, 0)
        mocks['getLogger'].return_value.warning.assert_called()

    def test_existing_lock_file_process_dead(self):
        extra = {
            'exists': patch('os.path.exists', return_value=True),
            'open': patch('builtins.open', mock_open(read_data="999")),
            'kill': patch('os.kill', side_effect=OSError)
        }
        _, mocks = self.load_module(extra)
        
        mocks['remove'].assert_called()

    def test_lock_file_logger_failure(self):
        extra = {
            'exists': patch('os.path.exists', return_value=True),
            'open': patch('builtins.open', mock_open(read_data="999")),
            'kill': patch('os.kill', return_value=None), # Process exists
        }
        # mock logger.warning to raise Exception
        
        mocks_holder = {}
        # We need to configure logger mock inside load_module, but logger is created during import.
        # We can patch logging.getLogger
        
        mock_logger = MagicMock()
        mock_logger.warning.side_effect = Exception("Logger Fail")
        
        extra['getLogger'] = patch('logging.getLogger', return_value=mock_logger)
        
        # We expect print to be called. Redirect stdout? 
        # Or patch builtins.print? No, print in module uses builtins.print.
        with patch('builtins.print') as mock_print:
            _, _ = self.load_module(extra, capture_mocks=mocks_holder)
            mock_print.assert_called()

    def test_step1_data_load_success(self):
        module, _ = self.load_module()
        
        with patch('app.pipeline.schedule_inference.data_load') as mock_dl, \
             patch('pandas.Timestamp.now'):
            
            mock_df = MagicMock()
            mock_dl.return_value = mock_df
            module.scheduler = MagicMock()
            
            module.step1_data_load()
            
            mock_dl.assert_called_once()
            mock_df.write_csv.assert_called_with(module.RAW_CSV)
            module.scheduler.add_job.assert_called()

    def test_step1_data_load_failure(self):
        module, _ = self.load_module()
        module.logger = MagicMock()
        
        with patch('app.pipeline.schedule_inference.data_load', side_effect=Exception("Error")):
            module.step1_data_load()
            module.logger.exception.assert_called()

    def test_step2_data_cleaning_success(self):
        module, _ = self.load_module()
        module.scheduler = MagicMock()
        
        with patch('pandas.read_csv') as mock_read, \
             patch('app.pipeline.schedule_inference.data_cleaning') as mock_dc:
            
            mock_df_in = MagicMock()
            mock_df_out = MagicMock()
            mock_read.return_value = mock_df_in
            mock_dc.return_value = mock_df_out
            
            module.step2_data_cleaning()
            
            mock_read.assert_called_with(module.RAW_CSV, low_memory=False)
            mock_dc.assert_called_with(mock_df_in)
            mock_df_out.to_csv.assert_called_with(module.CLEANED_CSV, index=False)
            module.scheduler.add_job.assert_called()

    def test_step2_data_cleaning_failure(self):
        module, _ = self.load_module()
        module.logger = MagicMock()
        with patch('pandas.read_csv', side_effect=Exception("Error")):
            module.step2_data_cleaning()
            module.logger.exception.assert_called()

    def test_step3_feature_engineering_success(self):
        module, _ = self.load_module()
        module.scheduler = MagicMock()
        
        with patch('pandas.read_csv') as mock_read, \
             patch('app.pipeline.schedule_inference.feature_engineering') as mock_fe:
            
            mock_read.return_value = MagicMock()
            mock_fe.return_value = MagicMock()
            
            module.step3_feature_engineering()
            
            # Should schedule Step 7 directly
            args, _ = module.scheduler.add_job.call_args
            self.assertEqual(args[0], module.step7_update_match_results)

    def test_step3_feature_engineering_failure(self):
        module, _ = self.load_module()
        module.logger = MagicMock()
        with patch('pandas.read_csv', side_effect=Exception("Error")):
            module.step3_feature_engineering()
            module.logger.exception.assert_called()

    def test_step6_generate_predictions_success(self):
        module, _ = self.load_module()
        
        with patch('app.pipeline.schedule_inference.generate_weekly_predictions') as mock_gen, \
             patch('app.pipeline.schedule_inference.step7_update_match_results') as mock_step7:
            
            module.step6_generate_predictions()
            
            mock_gen.assert_called()
            mock_step7.assert_called_once()
            self.assertFalse(module.step6_running)

    def test_step6_generate_predictions_failure(self):
        module, _ = self.load_module()
        module.logger = MagicMock()
        
        with patch('app.pipeline.schedule_inference.generate_weekly_predictions', side_effect=Exception("Error")):
            with self.assertRaises(Exception):
                module.step6_generate_predictions()
            
            self.assertFalse(module.step6_running)
            module.logger.exception.assert_called()

    def test_step7_update_match_results_simple(self):
        module, _ = self.load_module()
        module.step6_running = False
        module.scheduler = MagicMock()
        
        with patch('app.pipeline.schedule_inference.update_match_results') as mock_update:
            module.step7_update_match_results()
            mock_update.assert_called_once()
            module.scheduler.add_job.assert_called()

    def test_step7_waits_for_step6(self):
        module, _ = self.load_module()
        module.step6_running = True
        aug_1st = pd.Timestamp(year=2023, month=8, day=1, hour=1)
        module.scheduler = MagicMock()
        
        with patch('pandas.Timestamp.now', return_value=aug_1st), \
             patch('time.sleep') as mock_sleep, \
             patch('app.pipeline.schedule_inference.update_match_results') as mock_update:
            
            def stop_step6(*args):
                module.step6_running = False
            
            mock_sleep.side_effect = stop_step6
            
            module.step7_update_match_results()
            
            mock_sleep.assert_called()
            mock_update.assert_called()

    def test_step7_timeout_waiting_step6(self):
        module, mocks = self.load_module()
        module.step6_running = True
        aug_1st = pd.Timestamp(year=2023, month=8, day=1, hour=1)
        module.scheduler = MagicMock()
        
        with patch('pandas.Timestamp.now', return_value=aug_1st), \
             patch('time.sleep'), \
             patch('app.pipeline.schedule_inference.update_match_results') as mock_update:
            
            # Allow loop to run until timeout (waited >= 3600)
            # mock_sleep does nothing, so loop runs quickly (120 iterations of no-op)
            
            module.step7_update_match_results()
            
            # Verify timeout warning call
            mocks['getLogger'].return_value.warning.assert_called()
            mock_update.assert_called()

    def test_step8_update_future_predictions_success(self):
        module, _ = self.load_module()
        
        with patch('app.pipeline.schedule_inference.update_future_predictions') as mock_update:
            module.step8_update_future_predictions()
            mock_update.assert_called_once()

    def test_step8_failure(self):
        module, _ = self.load_module()
        module.logger = MagicMock()
        
        with patch('app.pipeline.schedule_inference.update_future_predictions', side_effect=Exception("X")):
            module.step8_update_future_predictions()
            module.logger.exception.assert_called()

    def test_logger_writer(self):
        module, _ = self.load_module()
        mock_logger = MagicMock()
        writer = module.LoggerWriter(mock_logger, level=logging.INFO)
        writer.write("Hello\nWorld")
        writer.flush()
        self.assertEqual(mock_logger.log.call_count, 2)

    def test_signal_handler(self):
        module, _ = self.load_module()
        module.scheduler = MagicMock()
        
        with patch('sys.exit') as mock_exit:
            module.signal_handler(signal.SIGINT, None)
            
            module.scheduler.shutdown.assert_called_with(wait=True)
            mock_exit.assert_called_with(0)

    def test_signal_handler_no_signum(self):
        module, _ = self.load_module()
        module.scheduler = MagicMock()
        
        with patch('sys.exit') as mock_exit:
            module.signal_handler(None, None)
            
            module.logger.info.assert_called()
            mock_exit.assert_called_with(0)

    def test_cleanup_lock(self):
        module, mocks = self.load_module()
        
        with patch('os.path.exists', return_value=True):
             module.cleanup_lock()
             
             mocks['remove'].assert_called_with(module.lock_file)

    def test_main_loop_keyboard_interrupt(self):
        with patch.dict(os.environ, {"SKIP_SCHEDULER_LOOP": "0"}):
             class ExitCalled(BaseException): pass  # NOSONAR
             
             extra = {
                 'sleep': patch('time.sleep', side_effect=KeyboardInterrupt),
                 'exit': patch('sys.exit', side_effect=ExitCalled)
             }
             
             mocks_holder = {}
             try:
                 _, _ = self.load_module(extra, capture_mocks=mocks_holder)
             except ExitCalled:
                 pass
             except KeyboardInterrupt:
                 self.fail("KeyboardInterrupt leaked!")
             
             mocks_holder['exit'].assert_called_with(0)

    def test_main_loop_generic_exception(self):
         with patch.dict(os.environ, {"SKIP_SCHEDULER_LOOP": "0"}):
             class LoopBreak(BaseException): pass  # NOSONAR
             
             extra = {
                 # [Exception(line 445), None(line 459), LoopBreak(line 445)]
                 'sleep': patch('time.sleep', side_effect=[Exception("Boom"), None, LoopBreak]),
                 'exit': patch('sys.exit')
             }
             
             mocks_holder = {}
             try:
                 _, _ = self.load_module(extra, capture_mocks=mocks_holder)
             except LoopBreak:
                 pass
             

             mocks_holder['getLogger'].return_value.exception.assert_called()
             mocks_holder['exit'].assert_not_called()

    def test_step7_failure(self):
        module, _ = self.load_module()
        module.logger = MagicMock()
        with patch('app.pipeline.schedule_inference.update_match_results', side_effect=Exception("Fail")):
            module.step7_update_match_results()
            module.logger.exception.assert_called()

    def test_step6_trigger_step7_failure(self):
        # Step 6 completes, tries to trigger step 7, step 7 fails
        module, _ = self.load_module()
        module.logger = MagicMock()
        
        # We need generate_weekly_predictions to succeed
        # And step7_update_match_results (called inside step6) to fail
        with patch('app.pipeline.schedule_inference.generate_weekly_predictions'), \
             patch('app.pipeline.schedule_inference.step7_update_match_results', side_effect=Exception("Trigger Fail")):
            
            module.step6_generate_predictions()
            
            # Should log STEP_7_FAILED_AFTER_STEP_6
            module.logger.exception.assert_called()

    def test_signal_handler_shutdown_error(self):
        module, _ = self.load_module()
        module.scheduler = MagicMock()
        module.scheduler.shutdown.side_effect = Exception("Shutdown Fail")
        module.logger = MagicMock()
        
        with patch('sys.exit') as mock_exit:
            module.signal_handler(signal.SIGINT, None)
            module.logger.exception.assert_called()
            mock_exit.assert_called_with(0)

    def test_cleanup_lock_error(self):
        module, _ = self.load_module()
        
        with patch('os.path.exists', return_value=True), \
             patch('os.remove', side_effect=OSError("Remove Fail")):
             
             module.cleanup_lock()
             # Should pass silently

    def test_main_loop_shutdown_error(self):
         with patch.dict(os.environ, {"SKIP_SCHEDULER_LOOP": "0"}):
             class ExitCalled(BaseException): pass  # NOSONAR
             
             extra = {
                 'sleep': patch('time.sleep', side_effect=KeyboardInterrupt),
                 'exit': patch('sys.exit', side_effect=ExitCalled)
             }
             
             mocks_holder = {}
             # We need to inject error into module.scheduler.shutdown
             # But module is not returned until load_module finishes (which is after loop exits)
             # But loop uses module.scheduler which is the mocked scheduler from import
             
             # We can Pre-configure the mock?
             # In import_schedule_inference, we verify 'scheduler' mock is created.
             # We can spy on it? Or configure side_effect on the mock returned by BackgroundScheduler?
             
             # The import_schedule_inference function creates `mock_apscheduler`.
             # `sys.modules['apscheduler...'].BackgroundScheduler` return value is the scheduler instance.
             # We can check `mocks` but we can't configure it easily via `extra_patches` unless we patch the CLASS.
             
             # We can patch BackgroundScheduler in extra_patches to return a configured mock!
             
             mock_scheduler_instance = MagicMock()
             mock_scheduler_instance.shutdown.side_effect = Exception("Shutdown Boom")
             mock_bg_scheduler = MagicMock(return_value=mock_scheduler_instance)
             
             def configure_scheduler(modules):
                 modules['apscheduler.schedulers.background'].BackgroundScheduler = mock_bg_scheduler
             
             try:
                 _, _ = self.load_module(extra, capture_mocks=mocks_holder, setup_hook=configure_scheduler)
             except ExitCalled:
                 pass
             except KeyboardInterrupt:
                 self.fail("KeyboardInterrupt leaked!")
             

             # Verify exception logged (it's inside except KeyboardInterrupt -> try/except Exception)
             # We can't check log message content easily but call count implies it.
             # Actually "logger.exception" is called for shutdown error? No, code says:
             # except Exception as shutdown_error: logger.exception(...)
             # mocks_holder['getLogger'].return_value.exception.assert_called()
             # except Exception as shutdown_error: logger.exception(...)
             mocks_holder['getLogger'].return_value.exception.assert_called()
             mocks_holder['exit'].assert_called_with(0)


if __name__ == '__main__':
    unittest.main()
# sync 1774962760705341726
