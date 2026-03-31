import { MapPin, Clock, Calendar } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { Card } from "@/components/ui/card";

export interface Match {
  id: string;
  date?: string;
  stadium?: string;
  time?: string;
  venue?: string;
  status?: string;
  isCompleted?: boolean;
  team1: {
    name: string;
    logo?: string;
    coach?: string;
    ourPrediction?: number;
    result?: string | number;
  };
  team2: {
    name: string;
    logo?: string;
    coach?: string;
    ourPrediction?: number;
    result?: string | number;
  };
}

interface MatchCardProps {
  match: Match;
  variant?: "detailed" | "compact";
  onClick?: () => void;
  week?: number | null;
  year?: number | null;
}

const MatchCard = ({ match, variant = "detailed", onClick, week, year }: MatchCardProps) => {
  const navigate = useNavigate();
  const hasResult = match.team1.result != null && match.team2.result != null;

  const handleClick = () => {
    if (onClick) {
      onClick();
    } else {
      // Navigate to match details with week/year preserved in URL
      const params = new URLSearchParams();
      if (week) params.set("week", week.toString());
      if (year) params.set("year", year.toString());
      const queryString = params.toString();
      navigate(`/match/${match.id}${queryString ? `?${queryString}` : ""}`);
    }
  };

  if (variant === "compact") {
    return (
      <Card
        className="p-4 border border-border rounded-2xl bg-card hover:shadow-md transition-shadow cursor-pointer"
        onClick={handleClick}
      >
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <img
              src={match.team1.logo || "/placeholder.svg"}
              alt={match.team1.name}
              className="w-10 h-10"
            />
            <span className="font-semibold text-sm">{match.team1.name}</span>
          </div>
          <div className="flex flex-col items-center gap-1">
            <span className="text-xs text-muted-foreground">Coach</span>
            <span className="text-sm">{match.team1.coach}</span>
          </div>
          {hasResult && (
            <div className="flex flex-col items-center gap-1">
              <span className="text-xs text-muted-foreground">Result</span>
              <span className="font-bold text-prediction-blue">{match.team1.result}</span>
            </div>
          )}
          <div className="flex flex-col items-center gap-1">
            <span className="text-xs text-muted-foreground">Our Prediction</span>
            <span className="font-bold text-prediction-blue bg-prediction-blue/10 px-4 py-1 rounded min-w-[90px] text-center">
              {match.team1.ourPrediction != null ? `${match.team1.ourPrediction}%` : "—"}
            </span>
          </div>
        </div>
        <div className="flex items-center justify-between gap-4 mt-3 pt-3 border-t border-border">
          <div className="flex items-center gap-3">
            <img
              src={match.team2.logo || "/placeholder.svg"}
              alt={match.team2.name}
              className="w-10 h-10"
            />
            <span className="font-semibold text-sm">{match.team2.name}</span>
          </div>
          <div className="flex flex-col items-center gap-1">
            <span className="text-sm">{match.team2.coach}</span>
          </div>
          {hasResult && (
            <div className="flex flex-col items-center gap-1">
              <span className="font-bold text-prediction-orange">{match.team2.result}</span>
            </div>
          )}
          <div className="flex flex-col items-center gap-1">
            <span className="font-bold text-prediction-orange bg-prediction-orange/10 px-4 py-1 rounded min-w-[90px] text-center">
              {match.team2.ourPrediction != null ? `${match.team2.ourPrediction}%` : "—"}
            </span>
          </div>
        </div>
      </Card>
    );
  }

  return (
    <Card
      className="p-6 border border-border rounded-2xl bg-card hover:shadow-lg transition-shadow cursor-pointer"
      onClick={handleClick}
    >
      {/* Date and Time Display */}
      {match.date && (
        <div className="flex items-center gap-3 mb-4 pb-3 border-b border-border">
          <Calendar className="w-5 h-5 text-muted-foreground" />
          <div className="flex items-center gap-2">
            <span className="text-base font-bold text-foreground">{match.date}</span>
            {match.time && (
              <span className="text-base font-bold text-foreground">{match.time}</span>
            )}
          </div>
        </div>
      )}

      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div className="flex items-center gap-2 text-sm text-stadium">
          <span className="font-semibold">{match.stadium || "Stadium TBA"}</span>
        </div>
        <div className="flex items-center gap-3 text-sm text-muted-foreground">
          <div className="flex items-center gap-1">
            <MapPin className="w-4 h-4" />
            <span>{match.venue || "Venue TBD"}</span>
          </div>
          {match.isCompleted && match.status && (
            <span className="px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/15 text-emerald-400">
              {match.status}
            </span>
          )}
        </div>
      </div>

      <div className="hidden md:block space-y-4">
        <div className="grid grid-cols-[1fr,1fr,1fr] gap-4 items-center">
          <div className="col-span-3 grid grid-cols-[1fr,1fr,1fr] gap-4 pb-2">
            <div className="flex items-center gap-3">
              <span className="text-sm font-medium text-muted-foreground">Teams</span>
            </div>
            <div className="text-left">
              <span className="text-xs font-medium text-muted-foreground">Coach</span>
            </div>
            <div className={`grid ${hasResult ? "grid-cols-2" : "grid-cols-1"} gap-4`}>
              {hasResult && (
                <span className="text-xs font-medium text-muted-foreground text-left">Result</span>
              )}
              <div className="flex flex-col gap-2 items-center">
                <span className="text-xs font-medium text-muted-foreground text-center">Our Prediction</span>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <img
              src={match.team1.logo || "/placeholder.svg"}
              alt={match.team1.name}
              className="w-12 h-12"
            />
            <span className="font-semibold">{match.team1.name}</span>
          </div>
          <div className="text-left">
            <span className="text-sm">{match.team1.coach || "Coach TBD"}</span>
          </div>
          <div className={`grid ${hasResult ? "grid-cols-2" : "grid-cols-1"} gap-4`}>
            {hasResult && (
              <span className="font-bold text-lg text-prediction-blue text-left">
                {match.team1.result}
              </span>
            )}
            <div className="flex items-center">
              <span className="font-bold text-sm text-prediction-blue bg-prediction-blue/10 px-4 py-2 rounded-lg text-center w-full">
                {match.team1.ourPrediction != null ? `${match.team1.ourPrediction}%` : "—"}
              </span>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <img
              src={match.team2.logo || "/placeholder.svg"}
              alt={match.team2.name}
              className="w-12 h-12"
            />
            <span className="font-semibold">{match.team2.name}</span>
          </div>
          <div className="text-left">
            <span className="text-sm">{match.team2.coach || "Coach TBD"}</span>
          </div>
          <div className={`grid ${hasResult ? "grid-cols-2" : "grid-cols-1"} gap-4`}>
            {hasResult && (
              <span className="font-bold text-lg text-prediction-orange text-left">
                {match.team2.result}
              </span>
            )}
            <div className="flex items-center">
              <span className="font-bold text-sm text-prediction-orange bg-prediction-orange/10 px-4 py-2 rounded-lg text-center w-full">
                {match.team2.ourPrediction != null ? `${match.team2.ourPrediction}%` : "—"}
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Mobile-friendly layout without coach column */}
      <div className="md:hidden rounded-2xl border border-border px-4 py-4 space-y-4">
        {[
          {
            label: match.team1.name,
            logo: match.team1.logo,
            result: match.team1.result,
            prediction: match.team1.ourPrediction,
            resultClass: "text-prediction-blue",
            predictionClass: "text-prediction-blue",
          },
          {
            label: match.team2.name,
            logo: match.team2.logo,
            result: match.team2.result,
            prediction: match.team2.ourPrediction,
            resultClass: "text-prediction-orange",
            predictionClass: "text-prediction-orange",
          },
        ].map((team) => (
          <div
            key={team.label}
            className="flex items-center justify-between gap-3"
          >
            <div className="flex items-center gap-3">
              <img
                src={team.logo || "/placeholder.svg"}
                alt={team.label}
                className="w-10 h-10"
              />
              <div>
                <p className="font-semibold text-foreground">{team.label}</p>
                {hasResult && team.result != null && (
                  <p className={`text-sm font-semibold ${team.resultClass}`}>
                    Result: {team.result}
                  </p>
                )}
              </div>
            </div>
            <div className="text-right">
              <p className="text-xs text-muted-foreground">Our Prediction</p>
              <p className={`font-bold ${team.predictionClass}`}>
                {team.prediction != null ? `${team.prediction}%` : "—"}
              </p>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
};

export default MatchCard;
