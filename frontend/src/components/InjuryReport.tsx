import { Card } from "@/components/ui/card";

interface Injury {
  athlete: {
    id: string;
    displayName: string;
    headshot?: string;
    position?: {
      abbreviation: string;
    };
  };
  status: {
    id: string;
    name: string;
    description?: string;
  };
  injury?: {
    type: string;
    date?: string;
  };
  practiceStatus?: string;
  estimatedReturn?: string;
}

interface InjuryReportProps {
  homeTeam: {
    name: string;
    logo?: string;
    injuries: Injury[];
  };
  awayTeam: {
    name: string;
    logo?: string;
    injuries: Injury[];
  };
}

const InjuryReport = ({ homeTeam, awayTeam }: InjuryReportProps) => {
  // Filter injuries to only show players who won't play (Inactive, IR, etc.)
  const filterNonPlayingInjuries = (injuries: Injury[]): Injury[] => {
    return injuries.filter((injury) => {
      const statusName = typeof injury.status?.name === 'string' 
        ? injury.status.name 
        : typeof injury.status === 'string' 
        ? injury.status 
        : '';
      
      const statusLower = statusName.toLowerCase();
      
      // Only show players who definitely won't play
      return (
        statusLower.includes("ir") || 
        statusLower.includes("inactive") ||
        statusLower.includes("reserve") ||
        statusLower.includes("out")
      );
    });
  };

  const getStatusColor = (status: string) => {
    const statusLower = status.toLowerCase();
    if (statusLower.includes("ir") || statusLower.includes("inactive")) {
      return "bg-red-500";
    }
    if (statusLower.includes("questionable") || statusLower.includes("doubtful")) {
      return "bg-orange-500";
    }
    if (statusLower.includes("probable")) {
      return "bg-yellow-500";
    }
    return "bg-green-500";
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return "TBD";
    try {
      const date = new Date(dateString);
      if (isNaN(date.getTime())) return dateString;
      return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    } catch {
      return dateString;
    }
  };

  const TeamInjuries = ({
    teamName,
    teamLogo,
    injuries,
  }: {
    teamName: string;
    teamLogo?: string;
    injuries: Injury[];
  }) => {
    if (injuries.length === 0) {
      return (
        <div className="text-center py-4">
          <p className="text-sm text-muted-foreground">No injuries reported</p>
        </div>
      );
    }

    return (
      <div className="space-y-2">
        {injuries.map((injury, index) => (
          <div
            key={injury.athlete.id || `injury-${index}`}
            className="flex items-center gap-3 py-2 border-b border-border last:border-0"
          >
            <div className="w-10 h-10 rounded-full bg-muted overflow-hidden flex items-center justify-center flex-shrink-0">
              {injury.athlete.headshot ? (
                <img
                  src={injury.athlete.headshot}
                  alt={injury.athlete.displayName || "Player"}
                  className="w-full h-full object-cover"
                />
              ) : (
                <span className="text-xs text-muted-foreground">
                  {(injury.athlete.displayName || "?").charAt(0).toUpperCase()}
                </span>
              )}
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span className="font-semibold text-sm text-foreground">
                  {typeof injury.athlete.displayName === 'string' ? injury.athlete.displayName : 'Unknown Player'}
                </span>
                <span className="text-xs text-muted-foreground">
                  {(() => {
                    const pos = injury.athlete.position;
                    if (typeof pos === 'string') return pos;
                    if (pos && typeof pos === 'object' && typeof pos.abbreviation === 'string') {
                      return pos.abbreviation;
                    }
                    return "";
                  })()}
                </span>
              </div>
              {injury.injury?.type && typeof injury.injury.type === 'string' && (
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-xs text-muted-foreground">
                    {injury.injury.type}
                  </span>
                </div>
              )}
            </div>
            <div className="text-left flex-shrink-0">
              <p className="text-xs text-muted-foreground">
                Est. Return: {formatDate(injury.estimatedReturn)}
              </p>
            </div>
          </div>
        ))}
      </div>
    );
  };

  return (
    <Card className="rounded-2xl border border-border bg-card p-6">
      <h2 className="text-xl font-bold uppercase tracking-wide text-foreground mb-6">
        Injury Report
      </h2>

      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-3 mb-4">
            {homeTeam.logo && (
              <img src={homeTeam.logo} alt={homeTeam.name} className="w-6 h-6" />
            )}
            <h3 className="font-semibold text-foreground">{homeTeam.name}</h3>
          </div>
          <div className="space-y-1">
            <div className="grid grid-cols-[1fr,auto] gap-4 text-xs font-semibold text-muted-foreground uppercase tracking-wide pb-2 border-b border-border">
              <span>Name</span>
              <span className="text-left">Est. Return</span>
            </div>
            <TeamInjuries
              teamName={homeTeam.name}
              teamLogo={homeTeam.logo}
              injuries={filterNonPlayingInjuries(homeTeam.injuries)}
            />
          </div>
        </div>

        <div>
          <div className="flex items-center gap-3 mb-4">
            {awayTeam.logo && (
              <img src={awayTeam.logo} alt={awayTeam.name} className="w-6 h-6" />
            )}
            <h3 className="font-semibold text-foreground">{awayTeam.name}</h3>
          </div>
          <div className="space-y-1">
            <div className="grid grid-cols-[1fr,auto] gap-4 text-xs font-semibold text-muted-foreground uppercase tracking-wide pb-2 border-b border-border">
              <span>Name</span>
              <span className="text-left">Est. Return</span>
            </div>
            <TeamInjuries
              teamName={awayTeam.name}
              teamLogo={awayTeam.logo}
              injuries={filterNonPlayingInjuries(awayTeam.injuries)}
            />
          </div>
        </div>
      </div>
    </Card>
  );
};

export default InjuryReport;

