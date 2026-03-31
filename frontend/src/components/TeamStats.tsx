import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

interface TeamStatsProps {
  homeTeam: {
    name: string;
    logo?: string;
    abbreviation?: string;
  };
  awayTeam: {
    name: string;
    logo?: string;
    abbreviation?: string;
  };
  stats: {
    totalYards?: { home: number; away: number };
    turnovers?: { home: number; away: number };
    firstDowns?: { home: number; away: number };
    penalties?: { home: string; away: string };
    thirdDown?: { home: string; away: string };
    fourthDown?: { home: string; away: string };
    redZone?: { home: string; away: string };
    possession?: { home: string; away: string };
  };
}

const TeamStats = ({ homeTeam, awayTeam, stats }: TeamStatsProps) => {
  // Extract team abbreviations from names if not provided
  const getAbbreviation = (name: string): string => {
    const abbreviations: { [key: string]: string } = {
      "New Orleans Saints": "NO",
      "Miami Dolphins": "MIA",
      "Kansas City Chiefs": "KC",
      "Dallas Cowboys": "DAL",
      "Detroit Lions": "DET",
      "Green Bay Packers": "GB",
      // Add more as needed
    };
    return abbreviations[name] || name.split(" ").map((w) => w[0]).join("").substring(0, 3).toUpperCase();
  };

  const homeAbbr = homeTeam.abbreviation || getAbbreviation(homeTeam.name);
  const awayAbbr = awayTeam.abbreviation || getAbbreviation(awayTeam.name);

  const formatStat = (stat: any): string => {
    if (stat == null || stat === undefined) return "—";
    if (typeof stat === "number") {
      if (isNaN(stat)) return "—";
      return String(stat);
    }
    if (typeof stat === "string") {
      if (stat === "NaN" || stat === "null" || stat === "undefined") return "—";
      return stat;
    }
    return "—";
  };

  const calculatePercentage = (home: number, away: number): number => {
    const total = home + away;
    if (total === 0) return 50;
    return (home / total) * 100;
  };
  
  // For non-numeric stats, calculate percentage based on first number in string (e.g., "4/12" -> 4)
  const parseStatValue = (stat: string | number | undefined): number => {
    if (typeof stat === "number") return stat;
    if (typeof stat === "string") {
      // Try to extract first number (e.g., "4/12" -> 4, "6-45" -> 6)
      const match = stat.match(/^(\d+)/);
      if (match) return Number(match[1]);
    }
    return 0;
  };

  const statItems = [
    {
      label: "Total Yards",
      home: stats.totalYards?.home,
      away: stats.totalYards?.away,
      isNumeric: true,
    },
    {
      label: "Turnovers",
      home: stats.turnovers?.home,
      away: stats.turnovers?.away,
      isNumeric: true,
    },
    {
      label: "1st Downs",
      home: stats.firstDowns?.home,
      away: stats.firstDowns?.away,
      isNumeric: true,
    },
    {
      label: "Penalties",
      home: stats.penalties?.home,
      away: stats.penalties?.away,
      isNumeric: false,
    },
    {
      label: "3rd Down",
      home: stats.thirdDown?.home,
      away: stats.thirdDown?.away,
      isNumeric: false,
    },
    {
      label: "4th Down",
      home: stats.fourthDown?.home,
      away: stats.fourthDown?.away,
      isNumeric: false,
    },
    {
      label: "Red Zone",
      home: stats.redZone?.home,
      away: stats.redZone?.away,
      isNumeric: false,
    },
    {
      label: "Possession",
      home: stats.possession?.home,
      away: stats.possession?.away,
      isNumeric: false,
    },
  ].filter((item) => {
    // Filter out items where both values are null, undefined, or NaN
    const homeValid = item.home != null && item.home !== undefined && !(typeof item.home === 'number' && isNaN(item.home));
    const awayValid = item.away != null && item.away !== undefined && !(typeof item.away === 'number' && isNaN(item.away));
    return homeValid || awayValid;
  });

  if (statItems.length === 0) {
    return null;
  }

  return (
    <Card className="border-border bg-card">
      <CardHeader className="pb-4">
        <CardTitle className="text-xl font-bold uppercase tracking-wide text-foreground mb-4">
          Team Stats
        </CardTitle>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            {homeTeam.logo && (
              <img
                src={homeTeam.logo}
                alt={homeTeam.name}
                className="w-9 h-9 object-contain"
              />
            )}
            <span className="font-semibold text-base text-foreground">{homeAbbr}</span>
          </div>
          <div className="flex items-center gap-2.5">
            {awayTeam.logo && (
              <img
                src={awayTeam.logo}
                alt={awayTeam.name}
                className="w-9 h-9 object-contain"
              />
            )}
            <span className="font-semibold text-base text-foreground">{awayAbbr}</span>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4 pt-0">
        {statItems.map((item, index) => {
          // For numeric stats, use the values directly
          // For string stats (like "4/12"), extract the first number for visualization
          let homeValue = item.isNumeric 
            ? (item.home as number) 
            : parseStatValue(item.home);
          let awayValue = item.isNumeric 
            ? (item.away as number) 
            : parseStatValue(item.away);
          
          // Ensure values are not NaN
          if (isNaN(homeValue)) homeValue = 0;
          if (isNaN(awayValue)) awayValue = 0;
          
          const homePercent = calculatePercentage(homeValue, awayValue);

          return (
            <div key={index} className="space-y-2.5">
              <div className="flex items-center justify-between gap-4">
                <span className="text-base font-bold text-foreground min-w-[70px] text-left">
                  {formatStat(item.home)}
                </span>
                <span className="text-xs font-medium text-muted-foreground flex-1 text-center uppercase tracking-wide">
                  {item.label}
                </span>
                <span className="text-base font-bold text-foreground min-w-[70px] text-right">
                  {formatStat(item.away)}
                </span>
              </div>
              <div className="relative h-3 bg-muted/60 rounded-full overflow-hidden">
                <div
                  className="absolute left-0 top-0 h-full bg-prediction-blue"
                  style={{
                    width: `${homePercent}%`,
                    backgroundImage: "repeating-linear-gradient(45deg, transparent, transparent 4px, rgba(255,255,255,0.2) 4px, rgba(255,255,255,0.2) 8px)",
                  }}
                />
                <div
                  className="absolute right-0 top-0 h-full bg-prediction-orange"
                  style={{
                    width: `${100 - homePercent}%`,
                    backgroundImage: "repeating-linear-gradient(45deg, transparent, transparent 4px, rgba(255,255,255,0.2) 4px, rgba(255,255,255,0.2) 8px)",
                  }}
                />
              </div>
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
};

export default TeamStats;

