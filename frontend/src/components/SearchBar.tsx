import { Search } from "lucide-react";
import { Input } from "@/components/ui/input";

interface SearchBarProps {
  value: string;
  onChange: (value: string) => void;
  selectedWeek: number;
}

const SearchBar = ({ value, onChange, selectedWeek }: SearchBarProps) => {
  return (
    <div className="bg-card border-2 border-primary rounded-lg p-6 mb-6">
      <div className="mb-2">
        <span className="text-sm font-semibold text-foreground uppercase tracking-wide">
          Week {selectedWeek}
        </span>
      </div>
      <div className="relative flex-1 w-full">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
        <Input
          type="text"
          placeholder="Search teams"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="pl-10 bg-background border-border"
        />
      </div>
    </div>
  );
};

export default SearchBar;
