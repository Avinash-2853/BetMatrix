import { Card } from "@/components/ui/card";
import { CheckCircle2, Target } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

interface AccuracySummaryProps {
  correct: number;
  total: number;
  percentage: number | null;
}

const AccuracySummary = ({ correct, total, percentage }: AccuracySummaryProps) => {
  const accuracyText =
    percentage != null ? `${percentage.toFixed(1)}%` : "No completed games yet";
  
  const tooltipMessage = total > 0
    ? `We have predicted ${correct} matches perfectly out of ${total}`
    : "We'll display accuracy once games have final scores.";

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Card className="border border-border bg-card rounded-2xl p-6 mb-6 cursor-help">
            <div className="flex flex-col gap-5 md:flex-row md:items-center md:justify-between">
              <div>
                <p className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
                  Prediction Accuracy
                </p>
                <h3 className="text-2xl font-bold text-foreground mt-1">{accuracyText}</h3>
                <p className="text-sm text-muted-foreground">
                  {total > 0
                    ? `Correct on ${correct} of ${total} completed games`
                    : "We'll display accuracy once games have final scores."}
                </p>
              </div>

              <div className="flex flex-col gap-3 sm:grid sm:grid-cols-2 sm:gap-4 md:flex md:flex-row md:items-center md:gap-4 w-full md:w-auto">
                <div className="flex items-center gap-3 rounded-xl border border-border/60 px-4 py-3 bg-card">
                  <div className="flex items-center justify-center w-10 h-10 rounded-full bg-prediction-blue/15">
                    <CheckCircle2 className="w-5 h-5 text-prediction-blue" />
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Correct Picks</p>
                    <p className="text-2xl font-semibold text-foreground">{correct}</p>
                  </div>
                </div>
                <div className="flex items-center gap-3 rounded-xl border border-border/60 px-4 py-3 bg-card">
                  <div className="flex items-center justify-center w-10 h-10 rounded-full bg-prediction-orange/10">
                    <Target className="w-5 h-5 text-prediction-orange" />
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Total Games</p>
                    <p className="text-2xl font-semibold text-foreground">{total}</p>
                  </div>
                </div>
              </div>
            </div>
          </Card>
        </TooltipTrigger>
        <TooltipContent>
          <p>{tooltipMessage}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
};

export default AccuracySummary;
