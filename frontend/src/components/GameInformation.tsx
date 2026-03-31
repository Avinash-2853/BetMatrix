import { Calendar, MapPin, Cloud } from "lucide-react";
import { Card } from "@/components/ui/card";
import { format } from "date-fns";

interface GameInformationProps {
  date?: string;
  time?: string;
  stadium?: string;
  location?: string;
  weather?: {
    temperature?: number;
    condition?: string;
    source?: string;
  };
  stadiumImage?: string;
}

const GameInformation = ({
  date,
  time,
  stadium,
  location,
  weather,
  stadiumImage,
}: GameInformationProps) => {
  const formatDate = () => {
    if (!date) return "";
    try {
      const dateObj = new Date(date);
      if (isNaN(dateObj.getTime())) return "";
      return format(dateObj, "EEEE, d MMM, yyyy");
    } catch {
      return date;
    }
  };

  const formatTime = () => {
    // If time is provided directly, use it
    if (time) {
      return time;
    }
    // Otherwise, extract from date
    if (date) {
      try {
        const dateObj = new Date(date);
        if (!isNaN(dateObj.getTime())) {
          return format(dateObj, "h:mm a");
        }
      } catch {
        return "";
      }
    }
    return "";
  };

  return (
    <Card className="rounded-2xl border border-border bg-card overflow-hidden">
      <div className="p-6 space-y-4">
        <h2 className="text-xl font-bold uppercase tracking-wide text-foreground">
          Game Information
        </h2>

        <div className="space-y-3">
          {date && (
            <div className="flex items-center gap-3 pb-3 border-b border-border">
              <Calendar className="w-5 h-5 text-muted-foreground" />
              <div className="flex items-center gap-2">
                <span className="text-base font-bold text-foreground">{formatDate()}</span>
                {(time || date) && formatTime() && (
                  <span className="text-base font-bold text-foreground">{formatTime()}</span>
                )}
              </div>
            </div>
          )}

          <div className="flex items-start justify-between">
            <div className="flex items-start gap-3">
              <MapPin className="w-5 h-5 text-muted-foreground mt-0.5" />
              <div>
                <p className="font-semibold text-foreground">{stadium || "Stadium TBA"}</p>
                {location && (
                  <p className="text-sm text-muted-foreground">{location}</p>
                )}
              </div>
            </div>

            {weather && (
              <div className="flex items-center gap-2 text-sm">
                {weather.condition && (
                  <Cloud className="w-4 h-4 text-muted-foreground" />
                )}
                {weather.temperature != null && (
                  <span className="text-foreground">{weather.temperature}°</span>
                )}
                {weather.source && (
                  <a
                    href={weather.source}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
                  >
                    AccuWeather
                    <svg
                      className="w-3 h-3"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
                      />
                    </svg>
                  </a>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {stadiumImage && (
        <div className="w-full h-64 md:h-96 relative overflow-hidden">
          <img
            src={stadiumImage}
            alt={stadium || "Stadium"}
            className="w-full h-full object-cover"
          />
        </div>
      )}
    </Card>
  );
};

export default GameInformation;

