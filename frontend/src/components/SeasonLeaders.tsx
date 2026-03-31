import { Card } from "@/components/ui/card";

interface Leader {
  athlete: {
    id: string;
    displayName: string;
    headshot?: string;
    position?: {
      abbreviation: string;
    };
  };
  value: number;
  displayValue: string;
  statistics?: {
    completions?: number;
    attempts?: number;
    touchdowns?: number;
    interceptions?: number;
    carries?: number;
    receptions?: number;
  };
}

interface SeasonLeadersProps {
  homeTeam: {
    name: string;
    logo?: string;
    leaders: {
      passingYards?: Leader;
      rushingYards?: Leader;
      receivingYards?: Leader;
      sacks?: Leader;
      tackles?: Leader;
    };
  };
  awayTeam: {
    name: string;
    logo?: string;
    leaders: {
      passingYards?: Leader;
      rushingYards?: Leader;
      receivingYards?: Leader;
      sacks?: Leader;
      tackles?: Leader;
    };
  };
  isGameLeaders?: boolean; // true for game leaders (completed matches), false for season leaders (upcoming)
}

const SeasonLeaders = ({ homeTeam, awayTeam, isGameLeaders = false }: SeasonLeadersProps) => {
  const categories = [
    {
      label: "Passing Yards",
      home: homeTeam.leaders.passingYards,
      away: awayTeam.leaders.passingYards,
      formatValue: (leader: Leader) => {
        const stats = leader.statistics;
        if (stats?.completions != null && stats?.attempts != null) {
          return `${stats.completions}/${stats.attempts}, ${stats.touchdowns || 0} TD, ${stats.interceptions || 0} INT`;
        }
        return leader.displayValue;
      },
    },
    {
      label: "Rushing Yards",
      home: homeTeam.leaders.rushingYards,
      away: awayTeam.leaders.rushingYards,
      formatValue: (leader: Leader) => {
        const stats = leader.statistics;
        if (stats?.carries != null) {
          return `${stats.carries} CAR, ${stats.touchdowns || 0} TD`;
        }
        return leader.displayValue;
      },
    },
    {
      label: "Receiving Yards",
      home: homeTeam.leaders.receivingYards,
      away: awayTeam.leaders.receivingYards,
      formatValue: (leader: Leader) => {
        const stats = leader.statistics;
        if (stats?.receptions != null) {
          return `${stats.receptions} REC, ${stats.touchdowns || 0} TD`;
        }
        return leader.displayValue;
      },
    },
    {
      label: "Sacks",
      home: homeTeam.leaders.sacks,
      away: awayTeam.leaders.sacks,
      formatValue: (leader: Leader) => leader.displayValue,
    },
    {
      label: "Tackles",
      home: homeTeam.leaders.tackles,
      away: awayTeam.leaders.tackles,
      formatValue: (leader: Leader) => leader.displayValue,
    },
  ];

  const displayedCategories = isGameLeaders ? categories.slice(0, 3) : categories;

  const LeaderCard = ({
    leader,
    teamName,
    teamLogo,
    formatValue,
  }: {
    leader?: Leader;
    teamName: string;
    teamLogo?: string;
    formatValue: (leader: Leader) => string;
  }) => {
    if (!leader) {
      return (
        <div className="flex flex-col items-center gap-2">
          <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center">
            {teamLogo ? (
              <img src={teamLogo} alt={teamName} className="w-12 h-12" />
            ) : (
              <span className="text-xs text-muted-foreground">N/A</span>
            )}
          </div>
          <div className="text-center">
            <p className="text-sm font-semibold text-muted-foreground">—</p>
            <p className="text-xs text-muted-foreground">No data</p>
          </div>
        </div>
      );
    }

    return (
      <div className="flex flex-col items-center gap-2">
        <div className="w-16 h-16 rounded-full bg-muted overflow-hidden flex items-center justify-center">
          {leader.athlete?.headshot ? (
            <img
              src={leader.athlete.headshot}
              alt={leader.athlete?.displayName || "Player"}
              className="w-full h-full object-cover"
            />
          ) : (
            <span className="text-xs text-muted-foreground">
              {(leader.athlete?.displayName || leader.athlete?.name || "?").charAt(0).toUpperCase()}
            </span>
          )}
        </div>
        <div className="text-center">
          <p className="text-sm font-semibold text-foreground">
            {leader.athlete?.displayName || leader.athlete?.name || "Unknown Player"}
          </p>
          <p className="text-xs text-muted-foreground">
            {leader.athlete?.position?.abbreviation || "—"}
          </p>
          <p className="text-xs text-muted-foreground mt-1">
            {formatValue(leader)}
          </p>
        </div>
      </div>
    );
  };

  return (
    <Card className="rounded-2xl border border-border bg-card p-6">
      <h2 className="text-xl font-bold uppercase tracking-wide text-foreground mb-6">
        {isGameLeaders ? "Game Leaders" : "Season Leaders"}
      </h2>

      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          {homeTeam.logo && (
            <img src={homeTeam.logo} alt={homeTeam.name} className="w-8 h-8" />
          )}
          <span className="font-semibold text-foreground">{homeTeam.name}</span>
        </div>
        <div className="flex items-center gap-3">
          {awayTeam.logo && (
            <img src={awayTeam.logo} alt={awayTeam.name} className="w-8 h-8" />
          )}
          <span className="font-semibold text-foreground">{awayTeam.name}</span>
        </div>
      </div>

      <div className="space-y-6">
        {displayedCategories.map((category) => (
          <div key={category.label} className="grid grid-cols-[1fr,auto,1fr] gap-4 items-center">
            <LeaderCard
              leader={category.home}
              teamName={homeTeam.name}
              teamLogo={homeTeam.logo}
              formatValue={category.formatValue}
            />
            <div className="flex flex-col items-center gap-1">
              <span className="text-2xl font-bold text-foreground">
                {category.home?.value || 0}
              </span>
              <span className="text-xs text-muted-foreground">{category.label}</span>
              <span className="text-2xl font-bold text-foreground">
                {category.away?.value || 0}
              </span>
            </div>
            <LeaderCard
              leader={category.away}
              teamName={awayTeam.name}
              teamLogo={awayTeam.logo}
              formatValue={category.formatValue}
            />
          </div>
        ))}
      </div>
    </Card>
  );
};

export default SeasonLeaders;

