import { FC } from "react";
import { Calendar } from "lucide-react";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

interface YearSelectorProps {
  selectedYear: number | null;
  availableYears: number[];
  onYearChange: (year: number) => void;
  isDisabled?: boolean;
}

const YearSelector: FC<YearSelectorProps> = ({
  selectedYear,
  availableYears,
  onYearChange,
  isDisabled = false,
}) => {
  const sortedYears = [...availableYears].sort((a, b) => b - a);
  const handleChange = (value: string) => {
    const numeric = Number(value);
    if (!Number.isNaN(numeric)) {
      onYearChange(numeric);
    }
  };

  return (
    <div className="bg-card border-b border-border px-6 py-6">
      <div className="max-w-7xl mx-auto">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-prediction-blue/80 to-prediction-orange/80 flex items-center justify-center shadow-md flex-shrink-0">
              <Calendar className="w-5 h-5 text-white" />
            </div>
            <div>
              <p className="text-sm font-semibold text-foreground uppercase tracking-wide">Season Year</p>
              <p className="text-xs text-foreground/70">
                Select a year to explore archived predictions.
              </p>
            </div>
          </div>
          <div className="relative w-full lg:w-auto lg:max-w-sm">
            <div className="absolute -top-2 left-4 bg-card px-2 text-[11px] font-semibold text-muted-foreground uppercase tracking-wide pointer-events-none z-10">
              Season
            </div>
            <Select
              value={selectedYear?.toString()}
              onValueChange={handleChange}
              disabled={isDisabled || sortedYears.length === 0}
            >
              <SelectTrigger className="w-full bg-background border border-border rounded-2xl pl-4 pr-4 py-3 font-semibold text-foreground focus:ring-2 focus:ring-prediction-blue/60 text-left shadow-sm justify-between h-12">
                <SelectValue placeholder={sortedYears.length === 0 ? "Loading seasons..." : "Choose a season"} />
              </SelectTrigger>
              <SelectContent className="bg-card border border-border rounded-2xl shadow-2xl text-foreground">
                {sortedYears.map((year) => (
                  <SelectItem
                    key={year}
                    value={year.toString()}
                    className="text-sm font-semibold rounded-xl my-1 px-4 py-2 pl-4 data-[state=checked]:bg-gradient-to-r data-[state=checked]:from-prediction-blue data-[state=checked]:to-prediction-orange data-[state=checked]:text-white [&>span:first-child]:hidden"
                  >
                    {year} Season
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>
    </div>
  );
};

export default YearSelector;

