import { useState, useEffect, useRef } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { getNFLSchedule } from "@/lib/api";

const TOTAL_WEEKS = 21;

/**
 * Calculate NFL week date ranges as fallback (if API fails).
 * NFL season typically starts the first Thursday after Labor Day (first Monday in September).
 * Each week runs from Tuesday to Monday.
 */
const calculateNFLWeekDates = (year: number): Array<{ week: number; dateRange: string }> => {
  // Find Labor Day (first Monday in September)
  const laborDay = new Date(year, 8, 1); // September 1st
  while (laborDay.getDay() !== 1) {
    laborDay.setDate(laborDay.getDate() + 1);
  }

  // NFL season starts the first Thursday after Labor Day
  const seasonStart = new Date(laborDay);
  while (seasonStart.getDay() !== 4) { // Thursday = 4
    seasonStart.setDate(seasonStart.getDate() + 1);
  }

  const weeks: Array<{ week: number; dateRange: string }> = [];

  for (let week = 1; week <= TOTAL_WEEKS; week++) {
    // Each week starts on Wednesday (1 day before Thursday)
    const weekStart = new Date(seasonStart);
    weekStart.setDate(weekStart.getDate() + (week - 1) * 7 - 1);

    // Week ends on Monday (6 days after Tuesday)
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekEnd.getDate() + 6);

    const formatDate = (date: Date): string => {
      const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      return `${months[date.getMonth()]} ${date.getDate()}`;
    };

    weeks.push({
      week,
      dateRange: `${formatDate(weekStart)} - ${formatDate(weekEnd)}`,
    });
  }

  return weeks;
};

/**
 * Extract date range from ESPN schedule data for a specific week
 */
const extractWeekDateRange = (scheduleData: any): string | null => {
  try {
    const schedule = scheduleData?.content?.schedule;
    if (!schedule) return null;

    const dates: Date[] = [];

    // Iterate through all dates in the schedule
    for (const dateKey in schedule) {
      const dateData = schedule[dateKey];
      const games = dateData?.games || [];

      for (const game of games) {
        const competition = game?.competitions?.[0];
        if (competition?.date) {
          const gameDate = new Date(competition.date);
          if (!Number.isNaN(gameDate.getTime())) {
            dates.push(gameDate);
          }
        }
      }
    }

    if (dates.length === 0) return null;

    // Find earliest and latest dates and normalize the window
    dates.sort((a, b) => a.getTime() - b.getTime());
    let earliest = dates[0];
    let latest = dates.at(-1);
    if (!latest) return null;

    const MS_IN_DAY = 24 * 60 * 60 * 1000;
    const diffDays = Math.round((latest.getTime() - earliest.getTime()) / MS_IN_DAY);

    // If the range is shorter than a typical NFL week (7 days),
    // extend it backward/forward so the display matches official week windows.
    if (diffDays < 6) {
      const adjustedStart = new Date(earliest);
      adjustedStart.setDate(adjustedStart.getDate() - 2);
      const adjustedEnd = new Date(adjustedStart);
      adjustedEnd.setDate(adjustedEnd.getDate() + 6);
      earliest = adjustedStart;
      latest = adjustedEnd;
    }

    const formatDate = (date: Date): string => {
      const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      return `${months[date.getMonth()]} ${date.getDate()}`;
    };

    return `${formatDate(earliest)} - ${formatDate(latest)}`;
  } catch (error) {
    console.error("Error extracting week date range:", error);
    return null;
  }
};



interface WeekNavigationProps {
  selectedWeek: number;
  selectedYear: number;
  onWeekChange: (week: number) => void;
}

const updateWeekInList = (weeks: Array<{ week: number; dateRange: string }>, targetWeek: number, dateRange: string) => {
  return weeks.map((w) => (w.week === targetWeek ? { ...w, dateRange } : w));
};

const WeekNavigation = ({ selectedWeek, selectedYear, onWeekChange }: WeekNavigationProps) => {
  // Initialize with calculated dates immediately
  const [weeks, setWeeks] = useState<Array<{ week: number; dateRange: string }>>(() =>
    calculateNFLWeekDates(selectedYear)
  );

  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const weekRefs = useRef<(HTMLButtonElement | null)[]>([]);

  // Update calculated dates if year changes
  useEffect(() => {
    setWeeks(calculateNFLWeekDates(selectedYear));
  }, [selectedYear]);

  // Lazy load specific week data when selected
  // This is a progressive enhancement - if it fails, we still have the calculated dates
  useEffect(() => {
    let isMounted = true;

    getNFLSchedule(selectedYear, selectedWeek)
      .then((scheduleData) => {
        const dateRange = extractWeekDateRange(scheduleData);
        if (isMounted && dateRange) {
          setWeeks((prevWeeks) => updateWeekInList(prevWeeks, selectedWeek, dateRange));
        }
      })
      .catch((error) => {
        console.warn(
          `Failed to fetch precise date for week ${selectedWeek}`,
          error
        );
      });

    return () => {
      isMounted = false;
    };
  }, [selectedYear, selectedWeek]);

  const scrollLeft = () => {
    if (scrollContainerRef.current) {
      scrollContainerRef.current.scrollBy({ left: -200, behavior: 'smooth' });
    }
  };

  const scrollRight = () => {
    if (scrollContainerRef.current) {
      scrollContainerRef.current.scrollBy({ left: 200, behavior: 'smooth' });
    }
  };

  useEffect(() => {
    const selectedRef = weekRefs.current[selectedWeek - 1];
    if (selectedRef) {
      selectedRef.scrollIntoView({
        behavior: 'smooth',
        inline: 'center',
        block: 'nearest',
      });
    }
  }, [selectedWeek, weeks]);

  return (
    <div className="bg-[#141414] border-y border-[#1f1f1f]">
      <div className="flex items-center gap-3 max-w-7xl mx-auto px-6">
        <button
          onClick={scrollLeft}
          className="flex-shrink-0 w-10 h-10 rounded-full bg-[#3a3a3a] border border-[#4a4a4a] hover:bg-[#4a4a4a] flex items-center justify-center transition-all text-white shadow-sm"
          aria-label="Scroll left"
        >
          <ChevronLeft className="w-5 h-5" />
        </button>
        <div
          ref={scrollContainerRef}
          className="flex items-center overflow-x-auto scrollbar-hide flex-1"
        >
          {weeks.map((week, index) => (
            <div key={week.week} className="flex items-stretch">
              <button
                onClick={() => onWeekChange(week.week)}
                className="flex flex-col items-center justify-center px-6 py-4 min-w-[150px] h-24 bg-transparent transition-all hover:bg-[#1f1f1f]"
                ref={(el) => { weekRefs.current[week.week - 1] = el; }}
              >
                <span className={`font-bold text-2xl leading-none whitespace-nowrap ${selectedWeek === week.week ? "text-white" : "text-[#6a6a6a]"
                  }`} style={{ fontFamily: "'Bebas Neue', sans-serif" }}>
                  WEEK {week.week}
                </span>
                <span className="text-sm mt-1.5 text-[#6a6a6a] leading-none whitespace-nowrap" style={{ fontFamily: "'Lato', sans-serif" }}>
                  {week.dateRange}
                </span>
              </button>
              {index < weeks.length - 1 && (
                <div className="w-px bg-[#1f1f1f] self-stretch" />
              )}
            </div>
          ))}
        </div>
        <button
          onClick={scrollRight}
          className="flex-shrink-0 w-10 h-10 rounded-full bg-[#3a3a3a] border border-[#4a4a4a] hover:bg-[#4a4a4a] flex items-center justify-center transition-all text-white shadow-sm"
          aria-label="Scroll right"
        >
          <ChevronRight className="w-5 h-5" />
        </button>
      </div>
    </div>
  );
};

export default WeekNavigation;
