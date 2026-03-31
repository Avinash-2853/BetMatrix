import { Card } from "@/components/ui/card";
import { format } from "date-fns";

interface GameResult {
  id: string;
  date: string;
  opponent: {
    name: string;
    logo?: string;
    isHome: boolean;
  };
  score: {
    team: number;
    opponent: number;
  };
  result: "W" | "L" | "T";
}

interface Last5GamesProps {
  homeTeam: {
    name: string;
    logo?: string;
    games: GameResult[];
  };
  awayTeam: {
    name: string;
    logo?: string;
    games: GameResult[];
  };
}

const Last5Games = ({ homeTeam, awayTeam }: Last5GamesProps) => {
  const getResultColor = (result: "W" | "L" | "T") => {
    switch (result) {
      case "W":
        return "bg-emerald-500 text-white";
      case "L":
        return "bg-red-500 text-white";
      case "T":
        return "bg-yellow-500 text-white";
      default:
        return "bg-muted text-foreground";
    }
  };

  const formatGameDate = (dateString: string) => {
    try {
      const date = new Date(dateString);
      if (isNaN(date.getTime())) return dateString;
      return format(date, "MMM d");
    } catch {
      return dateString;
    }
  };

  const getTeamAbbreviation = (teamName: string): string => {
    const abbreviations: { [key: string]: string } = {
      "New England Patriots": "NE",
      "New York Giants": "NYG",
      "New York Jets": "NYJ",
      "Cincinnati Bengals": "CIN",
      "Tampa Bay Buccaneers": "TB",
      "Atlanta Falcons": "ATL",
      "Cleveland Browns": "CLE",
      "Detroit Lions": "DET",
      "Green Bay Packers": "GB",
      "Chicago Bears": "CHI",
      "San Francisco 49ers": "SF",
      "Philadelphia Eagles": "PHI",
    };
    return abbreviations[teamName] || teamName.split(" ").map((w) => w[0]).join("").substring(0, 3).toUpperCase();
  };

  const TeamGames = ({
    teamName,
    teamLogo,
    games,
  }: {
    teamName: string;
    teamLogo?: string;
    games: GameResult[];
  }) => {
    if (games.length === 0) {
      return (
        <div className="text-center py-4">
          <p className="text-sm text-muted-foreground">No recent games</p>
        </div>
      );
    }

    return (
      <div className="space-y-0">
        {games.map((game, index) => (
          <div
            key={game.id}
            className={`flex items-center justify-between py-3 px-2 ${index < games.length - 1 ? "border-b border-border/60" : ""} hover:bg-muted/30 transition-colors`}
          >
            <div className="flex items-center gap-3 flex-1 min-w-0">
              <span className={`w-9 h-9 rounded flex items-center justify-center text-sm font-bold flex-shrink-0 ${getResultColor(game.result)}`}>
                {game.result}
              </span>
              <div className="flex items-center gap-2.5 flex-1 min-w-0">
                {game.opponent.logo ? (
                  <img
                    src={game.opponent.logo}
                    alt={game.opponent.name}
                    className="w-8 h-8 object-contain flex-shrink-0"
                  />
                ) : (
                  <div className="w-8 h-8 rounded-full bg-muted flex-shrink-0" />
                )}
                <span className="text-sm font-medium text-foreground truncate">
                  {game.opponent.isHome ? "vs" : "@"} {game.opponent.name}
                </span>
              </div>
            </div>
            <div className="flex items-center gap-4 flex-shrink-0">
              <span className="text-xs font-medium text-muted-foreground min-w-[50px] text-right">
                {formatGameDate(game.date)}
              </span>
              <span className="text-sm font-bold text-foreground min-w-[60px] text-right">
                {game.score.team}-{game.score.opponent}
              </span>
            </div>
          </div>
        ))}
      </div>
    );
  };

  const homeAbbr = getTeamAbbreviation(homeTeam.name);
  const awayAbbr = getTeamAbbreviation(awayTeam.name);

  return (
    <Card className="rounded-2xl border border-border bg-card p-6">
      <h2 className="text-xl font-bold uppercase tracking-wide text-foreground mb-6">
        Last 5 Games
      </h2>

      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-2.5 mb-4 pb-2 border-b border-border/60">
            {homeTeam.logo && (
              <img src={homeTeam.logo} alt={homeTeam.name} className="w-8 h-8 object-contain flex-shrink-0" />
            )}
            <h3 className="font-bold text-base text-foreground whitespace-nowrap">{homeAbbr}</h3>
          </div>
          <TeamGames
            teamName={homeTeam.name}
            teamLogo={homeTeam.logo}
            games={homeTeam.games}
          />
        </div>

        <div>
          <div className="flex items-center gap-2.5 mb-4 pb-2 border-b border-border/60">
            {awayTeam.logo && (
              <img src={awayTeam.logo} alt={awayTeam.name} className="w-8 h-8 object-contain flex-shrink-0" />
            )}
            <h3 className="font-bold text-base text-foreground whitespace-nowrap">{awayAbbr}</h3>
          </div>
          <TeamGames
            teamName={awayTeam.name}
            teamLogo={awayTeam.logo}
            games={awayTeam.games}
          />
        </div>
      </div>
    </Card>
  );
};

export default Last5Games;

