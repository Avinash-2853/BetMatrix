import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import Header from "@/components/Header";
import WeekNavigation from "@/components/WeekNavigation";
import SearchBar from "@/components/SearchBar";
import MatchCard, { Match } from "@/components/MatchCard";
import AccuracySummary from "@/components/AccuracySummary";
import YearSelector from "@/components/YearSelector";
import { useAvailableData, usePredictions } from "@/hooks/use-predictions";
import { useToast } from "@/hooks/use-toast";
import { getGameMetadata, type GameMetadata, type PredictionResponse } from "@/lib/api";

const toPercentage = (value?: number | null) => {
  if (value == null) return undefined;
  const normalized = value > 1 ? value : value * 100;
  return Number(normalized.toFixed(2));
};

/**
 * Format date string to "Friday, 14 Nov, 2025" format
 */
const formatGameDate = (dateString: string | null): string => {
  if (!dateString) return "";

  try {
    const date = new Date(dateString);
    if (Number.isNaN(date.getTime())) return "";

    const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

    const dayName = days[date.getDay()];
    const month = months[date.getMonth()];
    const day = date.getDate();
    const year = date.getFullYear();

    return `${dayName}, ${day} ${month}, ${year}`;
  } catch {
    // Return empty string on invalid date
    return "";
  }
};

interface MatchWithRawDate extends Match {
  rawDate: string | null;
}

const getSeasonStartDate = (year: number): Date => {
  // First Thursday after Labor Day (first Monday in September)
  const laborDay = new Date(year, 8, 1); // September 1
  while (laborDay.getDay() !== 1) {
    laborDay.setDate(laborDay.getDate() + 1);
  }
  const seasonStart = new Date(laborDay);
  while (seasonStart.getDay() !== 4) {
    seasonStart.setDate(seasonStart.getDate() + 1);
  }
  return seasonStart;
};

/**
 * Calculate which NFL week the current date falls into.
 */
const calculateCurrentWeek = (year: number, availableWeeks: number[]): number => {
  if (availableWeeks.length === 0) return 1;
  const today = new Date();
  today.setHours(0, 0, 0, 0); // Normalize to start of day

  const seasonStart = getSeasonStartDate(year);
  const minWeek = Math.min(...availableWeeks);
  const maxWeek = Math.max(...availableWeeks);

  // If today is before season start, return first available week
  if (today < seasonStart) {
    return minWeek;
  }

  // Calculate week start dates (Tuesday of each week)
  const week1Start = new Date(seasonStart);
  week1Start.setDate(week1Start.getDate() - 2); // Tuesday before first Thursday

  // Find which week today falls into
  for (let week = 1; week <= maxWeek; week++) {
    const weekStart = new Date(week1Start);
    weekStart.setDate(weekStart.getDate() + (week - 1) * 7);

    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekEnd.getDate() + 6); // Monday
    weekEnd.setHours(23, 59, 59, 999);

    if (today >= weekStart && today <= weekEnd) {
      return availableWeeks.includes(week)
        ? week
        : availableWeeks.reduce((prev, curr) => Math.abs(curr - week) < Math.abs(prev - week) ? curr : prev, availableWeeks[0]);
    }
  }

  // If we're past all weeks or before week 1, clamp to min/max
  return today < week1Start ? minWeek : maxWeek;
};

const getGameTime = (gameDate: string | null): string | undefined => {
  if (!gameDate) return undefined;
  try {
    const dateObj = new Date(gameDate);
    if (!Number.isNaN(dateObj.getTime())) {
      return dateObj.toLocaleTimeString("en-US", {
        hour: "numeric",
        minute: "2-digit",
        hour12: true
      });
    }
  } catch {
    // Ignore time parsing errors
  }
  return undefined;
};

const checkIfMatchInPast = (gameDate: string | null): boolean => {
  if (!gameDate) return false;
  try {
    const matchDateObj = new Date(gameDate);
    const currentDate = new Date();
    return matchDateObj < currentDate;
  } catch {
    // Return false if date parsing fails
    return false;
  }
};

const mapPredictionToMatch = (
  prediction: PredictionResponse,
  gameDate: string | null,
  metadata?: GameMetadata | null
): MatchWithRawDate => {
  const formattedDate = formatGameDate(gameDate);
  const gameTime = getGameTime(gameDate);

  // Prioritize ESPN API scores over database scores
  const metadataScoresAvailable =
    metadata?.homeScore != null &&
    metadata?.awayScore != null;

  const homeScore = metadataScoresAvailable ? metadata?.homeScore : (prediction.home_score ?? null);
  const awayScore = metadataScoresAvailable ? metadata?.awayScore : (prediction.away_score ?? null);

  const isMatchDateInPast = checkIfMatchInPast(gameDate);
  const hasScores = isMatchDateInPast && homeScore != null && awayScore != null;

  // Debug logging for completed matches
  if (metadata?.isCompleted && !hasScores && isMatchDateInPast) {
    console.warn(`[mapPredictionToMatch] Completed match ${prediction.game_id} has no scores:`, {
      metadataHomeScore: metadata?.homeScore,
      metadataAwayScore: metadata?.awayScore,
      dbHomeScore: prediction.home_score,
      dbAwayScore: prediction.away_score,
    });
  }

  return {
    id: prediction.game_id,
    date: formattedDate || `Week ${prediction.week}, ${prediction.year}`,
    rawDate: gameDate,
    stadium: prediction.stadium,
    time: gameTime,
    venue: prediction.stadium,
    status: metadata?.isCompleted ? (metadata?.statusText ?? "FINAL") : undefined,
    isCompleted: Boolean(metadata?.isCompleted),
    team1: {
      name: prediction.home_team,
      logo: prediction.home_team_image_url,
      coach: prediction.home_coach,
      ourPrediction: toPercentage(prediction.home_team_win_probability),
      result: hasScores ? String(homeScore ?? "") : undefined,
    },
    team2: {
      name: prediction.away_team,
      logo: prediction.away_team_image_url,
      coach: prediction.away_coach,
      ourPrediction: toPercentage(prediction.away_team_win_probability),
      result: hasScores ? String(awayScore ?? "") : undefined,
    },
  };
};

const MatchList = ({ matches, selectedWeek, selectedYear }: { matches: MatchWithRawDate[], selectedWeek: number | null, selectedYear: number | null }) => {
  // Group matches by date
  const groupedByDate = new Map<string, MatchWithRawDate[]>();
  matches.forEach((match) => {
    const dateKey = match.date || 'TBD';
    if (!groupedByDate.has(dateKey)) {
      groupedByDate.set(dateKey, []);
    }
    groupedByDate.get(dateKey)?.push(match);
  });

  // Sort dates chronologically using raw dates
  const sortedDates = Array.from(groupedByDate.keys()).sort((a, b) => {
    const matchesA = groupedByDate.get(a);
    const matchesB = groupedByDate.get(b);
    const rawDateA = matchesA?.[0]?.rawDate;
    const rawDateB = matchesB?.[0]?.rawDate;

    if (rawDateA && rawDateB) {
      const dateA = new Date(rawDateA);
      const dateB = new Date(rawDateB);
      if (!Number.isNaN(dateA.getTime()) && !Number.isNaN(dateB.getTime())) {
        return dateA.getTime() - dateB.getTime();
      }
    }
    return a.localeCompare(b);
  });

  return (
    <div className="space-y-6">
      {sortedDates.map((dateKey) => (
        <div key={dateKey} className="space-y-4">
          <h2 className="text-lg font-semibold mb-4 text-foreground" style={{ fontFamily: "'Lato', sans-serif" }}>
            {dateKey}
          </h2>
          {groupedByDate.get(dateKey)?.map((match) => (
            <MatchCard
              key={match.id}
              match={match}
              variant="detailed"
              week={selectedWeek}
              year={selectedYear}
            />
          ))}
        </div>
      ))}
    </div>
  );
};

const RenderContent = ({
  isLoading,
  matches,
  accuracyStats,
  totalMatches,
  selectedWeek,
  selectedYear
}: {
  isLoading: boolean,
  matches: MatchWithRawDate[],
  accuracyStats: { total: number, correct: number, percentage: number | null },
  totalMatches: number,
  selectedWeek: number | null,
  selectedYear: number | null
}) => {
  if (isLoading) {
    return (
      <div className="text-center py-12">
        <p className="text-muted-foreground text-lg">Loading predictions...</p>
      </div>
    );
  }

  if (matches.length === 0) {
    return (
      <div className="text-center py-12">
        <p className="text-muted-foreground text-lg">No matches found for your search.</p>
      </div>
    );
  }

  return (
    <>
      {accuracyStats.total > 0 && (
        <AccuracySummary
          correct={accuracyStats.correct}
          total={accuracyStats.total}
          percentage={accuracyStats.percentage}
        />
      )}
      <MatchList matches={matches} selectedWeek={selectedWeek} selectedYear={selectedYear} />
    </>
  );
};

const Index = () => {
  const { toast } = useToast();
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchQuery, setSearchQuery] = useState("");
  const [gameMetadata, setGameMetadata] = useState<Map<string, GameMetadata>>(new Map());

  const availableDataQuery = useAvailableData();

  const selectedYear = useMemo(() => {
    const yearParam = searchParams.get("year");
    return yearParam ? Number.parseInt(yearParam, 10) : null;
  }, [searchParams]);

  const selectedWeek = useMemo(() => {
    const weekParam = searchParams.get("week");
    return weekParam ? Number.parseInt(weekParam, 10) : null;
  }, [searchParams]);

  const predictionsQuery = usePredictions(selectedYear, selectedWeek);

  // Update URL params when year/week changes, or set defaults if not in URL
  useEffect(() => {
    if (
      availableDataQuery.data &&
      availableDataQuery.data.years.length > 0 &&
      availableDataQuery.data.weeks.length > 0
    ) {
      const { years, weeks } = availableDataQuery.data;
      const today = new Date();
      const currentYear = today.getFullYear();
      const hasCurrentYear = years.includes(currentYear);
      const defaultYear = hasCurrentYear ? currentYear : Math.max(...years);
      const defaultWeek = (hasCurrentYear || defaultYear === Math.max(...years))
        ? calculateCurrentWeek(defaultYear, weeks)
        : Math.max(...weeks);

      // Only update URL if params are missing
      if (selectedYear === null || selectedWeek === null) {
        const newParams = new URLSearchParams(searchParams);
        if (selectedYear === null) newParams.set("year", defaultYear.toString());
        if (selectedWeek === null) newParams.set("week", defaultWeek.toString());
        setSearchParams(newParams, { replace: true });
      }
    }
  }, [availableDataQuery.data, selectedYear, selectedWeek, searchParams, setSearchParams]);

  useEffect(() => {
    if (predictionsQuery.isError && predictionsQuery.error) {
      toast({
        title: "Unable to load predictions",
        description: predictionsQuery.error instanceof Error ? predictionsQuery.error.message : "Unknown error",
      });
    }
  }, [predictionsQuery.isError, predictionsQuery.error, toast]);

  // Fetch match results from ESPN API for all predictions
  useEffect(() => {
    const predictions = predictionsQuery.data?.predictions ?? [];
    if (predictions.length === 0) {
      setGameMetadata(new Map());
      return;
    }

    let isCancelled = false;

    const fetchMetadata = async () => {
      const metadataMap = new Map<string, GameMetadata>();
      await Promise.all(
        predictions.map(async (prediction: PredictionResponse) => {
          try {
            // Fetch match results from ESPN API
            const metadata = await getGameMetadata(prediction.game_id);
            if (metadata) {
              metadataMap.set(prediction.game_id, metadata);
            }
          } catch (error) {
            console.warn(`Failed to load match results from ESPN API for ${prediction.game_id}`, error);
          }
        })
      );

      if (!isCancelled) {
        setGameMetadata(metadataMap);
      }
    };

    fetchMetadata();

    return () => {
      isCancelled = true;
    };
  }, [predictionsQuery.data]);

  const availableYears = useMemo(() => availableDataQuery.data?.years ?? [], [availableDataQuery.data]);
  const availableWeeks = useMemo(() => availableDataQuery.data?.weeks ?? [], [availableDataQuery.data]);

  const handleYearChange = (year: number) => {
    const newParams = new URLSearchParams(searchParams);
    newParams.set("year", year.toString());
    if (availableWeeks.length > 0) {
      const nextWeek = calculateCurrentWeek(year, availableWeeks);
      newParams.set("week", nextWeek.toString());
    }
    setSearchParams(newParams);
  };

  const handleWeekChange = (week: number) => {
    const newParams = new URLSearchParams(searchParams);
    newParams.set("week", week.toString());
    setSearchParams(newParams);
  };

  const matches = useMemo(() => {
    const predictions = predictionsQuery.data?.predictions ?? [];
    return predictions.map((prediction: PredictionResponse) => {
      const metadata = gameMetadata.get(prediction.game_id);
      const gameDate = metadata?.date || null;
      return mapPredictionToMatch(prediction, gameDate, metadata);
    });
  }, [predictionsQuery.data, gameMetadata]);

  const accuracyStats = useMemo(() => {
    const completed = matches.filter(
      (match) => match.team1.result != null && match.team2.result != null
    );

    const parseScore = (score: string | number | undefined) => {
      if (score == null) return null;
      const numeric = typeof score === "number" ? score : Number(score);
      return Number.isFinite(numeric) ? numeric : null;
    };

    let correct = 0;
    completed.forEach((match) => {
      const team1Score = parseScore(match.team1.result);
      const team2Score = parseScore(match.team2.result);
      if (team1Score == null || team2Score == null || team1Score === team2Score) {
        return;
      }
      const actualWinner = team1Score > team2Score ? match.team1.name : match.team2.name;
      const predictedWinner =
        (match.team1.ourPrediction ?? 0) >= (match.team2.ourPrediction ?? 0)
          ? match.team1.name
          : match.team2.name;
      if (actualWinner === predictedWinner) {
        correct += 1;
      }
    });

    const total = completed.length;
    const percentage = total > 0 ? (correct / total) * 100 : null;
    return { total, correct, percentage };
  }, [matches]);

  const filteredMatches = matches.filter((match) => {
    const searchLower = searchQuery.toLowerCase();
    return (
      match.team1.name.toLowerCase().includes(searchLower) ||
      match.team2.name.toLowerCase().includes(searchLower) ||
      (match.date?.toLowerCase().includes(searchLower)) ||
      (match.stadium?.toLowerCase().includes(searchLower))
    );
  });

  const isLoading = predictionsQuery.isLoading || availableDataQuery.isLoading;

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <YearSelector
        selectedYear={selectedYear}
        availableYears={availableYears}
        onYearChange={handleYearChange}
        isDisabled={availableDataQuery.isLoading}
      />
      <WeekNavigation
        selectedWeek={selectedWeek ?? 1}
        selectedYear={selectedYear ?? new Date().getFullYear()}
        onWeekChange={handleWeekChange}
      />

      <main className="max-w-7xl mx-auto px-4 py-8">
        <SearchBar
          value={searchQuery}
          onChange={setSearchQuery}
          selectedWeek={selectedWeek ?? 1}
        />

        <RenderContent
          isLoading={isLoading}
          matches={filteredMatches}
          accuracyStats={accuracyStats}
          totalMatches={filteredMatches.length}
          selectedWeek={selectedWeek}
          selectedYear={selectedYear}
        />
      </main>
    </div>
  );
};

export default Index;
