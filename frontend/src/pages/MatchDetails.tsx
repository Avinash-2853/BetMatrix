import { useParams, useNavigate, useSearchParams } from "react-router-dom";
import { useEffect, useState } from "react";
import { ArrowLeft, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import Header from "@/components/Header";
import GameInformation from "@/components/GameInformation";
import SeasonLeaders from "@/components/SeasonLeaders";
import InjuryReport from "@/components/InjuryReport";
import Last5Games from "@/components/Last5Games";
import { getPredictionByGameId, type PredictionResponse } from "@/lib/api";
import {
  getGameDetails,
  getTeamInfo,
  getTeamLeaders,
  getTeamEvents,
  getTeamInjuries,
  getGameLeaders,
  getTeamStatistics,
} from "@/lib/api";
import TeamStats from "@/components/TeamStats";
import { useToast } from "@/hooks/use-toast";

/**
 * Convert HTTP URLs to HTTPS to avoid mixed content errors
 * when fetching from ESPN API $ref links that come in responses
 */
const ensureHttps = (url: string): string => {
  if (url && typeof url === 'string' && url.startsWith('http://')) {
    return url.replace('http://', 'https://');
  }
  return url;
};

interface MatchDetailsData {
  gameInfo: {
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
  };
  seasonLeaders: {
    homeTeam: {
      name: string;
      logo?: string;
      leaders: any;
    };
    awayTeam: {
      name: string;
      logo?: string;
      leaders: any;
    };
    isGameLeaders?: boolean; // true for completed matches (game leaders), false for upcoming (season leaders)
  };
  injuries: {
    homeTeam: {
      name: string;
      logo?: string;
      injuries: any[];
    };
    awayTeam: {
      name: string;
      logo?: string;
      injuries: any[];
    };
  };
  last5Games: {
    homeTeam: {
      name: string;
      logo?: string;
      games: any[];
    };
    awayTeam: {
      name: string;
      logo?: string;
      games: any[];
    };
  };
  teamStats?: {
    homeTeam: {
      name: string;
      logo?: string;
      abbreviation?: string;
      totalYards?: number;
      turnovers?: number;
      firstDowns?: number;
      penalties?: string;
      thirdDown?: string;
      fourthDown?: string;
      redZone?: string;
      possession?: string;
    };
    awayTeam: {
      name: string;
      logo?: string;
      abbreviation?: string;
      totalYards?: number;
      turnovers?: number;
      firstDowns?: number;
      penalties?: string;
      thirdDown?: string;
      fourthDown?: string;
      redZone?: string;
      possession?: string;
    };
  };
}

const MatchDetails = () => {
  const { matchId } = useParams<{ matchId: string }>();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { toast } = useToast();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [matchData, setMatchData] = useState<PredictionResponse | null>(null);
  const [details, setDetails] = useState<MatchDetailsData | null>(null);

  // Get week and year from URL params to preserve when navigating back
  const week = searchParams.get("week");
  const year = searchParams.get("year");

  // Helper function to navigate back with preserved week/year
  const navigateBack = () => {
    const params = new URLSearchParams();
    if (week) params.set("week", week);
    if (year) params.set("year", year);
    const queryString = params.toString();
    navigate(`/predictions${queryString ? `?${queryString}` : ""}`);
  };

  useEffect(() => {
    const fetchMatchDetails = async () => {
      if (!matchId) {
        toast({
          title: "Error",
          description: "Match ID is required",
          variant: "destructive",
        });
        navigateBack();
        return;
      }

      try {
        setLoading(true);

        // Fetch prediction and game details in parallel for faster initial load
        const [prediction, gameDetails] = await Promise.all([
          getPredictionByGameId(matchId),
          getGameDetails(matchId).catch(() => null), // Don't fail if ESPN API fails
        ]);

        if (!prediction) {
          toast({
            title: "Match not found",
            description: "Could not find the requested match",
            variant: "destructive",
          });
          navigateBack();
          return;
        }

        setMatchData(prediction);

        // Simple in-memory caches so we don't re-fetch the same $ref repeatedly within a single render
        const athleteCache = new Map<string, any>();
        const statsCache = new Map<string, any>();

        // Helper function to fetch athlete details from $ref (defined early so it can be used in transformLeaders)
        const fetchAthleteFromRef = async (athleteRef: string): Promise<any> => {
          if (!athleteRef) return null;
          if (athleteCache.has(athleteRef)) {
            return athleteCache.get(athleteRef);
          }

          try {
            const response = await fetch(ensureHttps(athleteRef));
            if (response.ok) {
              const data = await response.json();
              const normalized = {
                id: data.id || "",
                displayName: data.displayName || data.fullName || data.name || "Unknown Player",
                headshot: data.headshot?.href || data.headshot || data.images?.[0]?.href,
                position: data.position || { abbreviation: data.positionAbbr || "" },
              };
              athleteCache.set(athleteRef, normalized);
              return normalized;
            }
          } catch (error) {
            console.warn(`Failed to fetch athlete from ${athleteRef}:`, error);
          }
          athleteCache.set(athleteRef, null);
          return null;
        };

        const fetchStatsFromRef = async (statsRef: string): Promise<any> => {
          if (!statsRef) return null;
          if (statsCache.has(statsRef)) return statsCache.get(statsRef);

          try {
            const response = await fetch(ensureHttps(statsRef));
            if (response.ok) {
              const data = await response.json();
              statsCache.set(statsRef, data);
              return data;
            }
          } catch (error) {
            console.warn(`Failed to fetch statistics from ${statsRef}:`, error);
          }

          statsCache.set(statsRef, null);
          return null;
        };

        const extractStatFromDetails = (statsData: any, preferredStats: string[]) => {
          if (!statsData) return null;
          const categories = statsData.splits?.categories || [];
          const preferred = preferredStats.map((s) => s.toLowerCase());

          for (const category of categories) {
            const statsArray = category.stats || [];
            for (const stat of statsArray) {
              const statName = stat.name?.toLowerCase();
              if (statName && preferred.includes(statName)) {
                return {
                  value: stat.value ?? 0,
                  displayValue: stat.displayValue ?? String(stat.value ?? 0),
                };
              }
            }
          }
          return null;
        };

        // Helper function to transform leaders data (defined early)
        const transformLeaders = async (leadersData: any) => {
          if (!leadersData) {
            console.warn("[transformLeaders] No leaders data provided");
            return {};
          }

          const leaders: any = {};
          
          // Try different possible structures from ESPN API
          let items: any[] = [];
          
          // Structure 1: items array directly
          if (Array.isArray(leadersData.items)) {
            items = leadersData.items;
          }
          // Structure 2: categories array
          else if (Array.isArray(leadersData.categories)) {
            items = leadersData.categories;
          }
          // Structure 3: data.items
          else if (Array.isArray(leadersData.data?.items)) {
            items = leadersData.data.items;
          }
          // Structure 4: leadersData is already an array
          else if (Array.isArray(leadersData)) {
            items = leadersData;
          }
          // Structure 5: Check for nested structure
          else if (leadersData.leaders && Array.isArray(leadersData.leaders)) {
            items = leadersData.leaders;
          }
          // Structure 6: Check for statistics array (common in game stats)
          else if (Array.isArray(leadersData.statistics)) {
            items = leadersData.statistics;
          }
          // Structure 7: Check for stats array
          else if (Array.isArray(leadersData.stats)) {
            items = leadersData.stats;
          }
          
          if (items.length === 0) {
            console.warn("[transformLeaders] No items found in leaders data, keys:", Object.keys(leadersData));
            return {};
          }
          

          // Process items and fetch athlete details for $ref links in parallel
          // First, collect all the fetch promises
          const leaderPromises = items.map(async (category, index) => {
            if (!category) return null;
            
            // Get the first leader from the category
            let leader = null;
            
            if (Array.isArray(category.leaders) && category.leaders.length > 0) {
              leader = category.leaders[0];
            } else if (category.leader) {
              leader = category.leader;
            } else if (category.athlete) {
              // Sometimes the category itself is the leader
              leader = category;
            }
            
            if (!leader) {
              return null;
            }
            const categoryName = (category.displayName || category.name || category.statName || "").toLowerCase();
            const preferredStats: string[] = [];
            let leaderKey: keyof typeof leaders | null = null;

            if (categoryName.includes("passing")) {
              preferredStats.push("passingyards", "netpassingyards");
              leaderKey = "passingYards";
            } else if (categoryName.includes("rushing")) {
              preferredStats.push("rushingyards", "netrushingyards");
              leaderKey = "rushingYards";
            } else if (categoryName.includes("receiving")) {
              preferredStats.push("receivingyards");
              leaderKey = "receivingYards";
            } else if (categoryName.includes("sack")) {
              preferredStats.push("sacks", "totalsacks");
              leaderKey = "sacks";
            } else if (categoryName.includes("tackle")) {
              preferredStats.push("totaltackles", "tackles", "solotackles");
              leaderKey = "tackles";
            }
            
            // Extract athlete information - ESPN API can have it in various places
            let athlete = null;
            
            // Collect athlete refs to fetch in parallel
            const athleteRefs: string[] = [];
            
            // Try athletes array (common in boxscore/game leader responses)
            if (Array.isArray(leader.athletes) && leader.athletes.length > 0) {
              const firstAthlete = leader.athletes[0];
              if (firstAthlete?.athlete) {
                athlete = firstAthlete.athlete;
              } else if (firstAthlete?.$ref) {
                athleteRefs.push(firstAthlete.$ref);
              }
            }
            
            // Try different athlete locations - check all possible nested structures
            if (leader.athlete) {
              // If athlete is an object with data, use it directly
              if (typeof leader.athlete === 'object') {
                if (leader.athlete.displayName || leader.athlete.name || leader.athlete.fullName) {
                  athlete = leader.athlete;
                } else if (leader.athlete.$ref) {
                  athleteRefs.push(leader.athlete.$ref);
                }
              }
            }
            
            // Try player object
            if (!athlete && leader.player) {
              if (typeof leader.player === 'object' && (leader.player.displayName || leader.player.name)) {
                athlete = leader.player;
              }
            }
            
            // Try competitor object  
            if (!athlete && leader.competitor) {
              if (typeof leader.competitor === 'object' && (leader.competitor.displayName || leader.competitor.name)) {
                athlete = leader.competitor;
              }
            }
            
            // Try if leader itself has athlete properties directly
            if (!athlete && (leader.displayName || leader.name || leader.fullName || leader.shortName)) {
              athlete = {
                id: leader.id || leader.athleteId || leader.playerId || "",
                displayName: leader.displayName || leader.name || leader.fullName || leader.shortName,
                headshot: leader.headshot || leader.image || leader.photo || leader.headshotUrl || leader.images?.[0]?.href,
                position: leader.position || { 
                  abbreviation: leader.positionAbbr || leader.position?.abbreviation || leader.pos || "" 
                },
              };
            }
            
            // Check if category has athlete info
            if (!athlete && category.athlete) {
              if (typeof category.athlete === 'object' && (category.athlete.displayName || category.athlete.name)) {
                athlete = category.athlete;
              }
            }
            
            // Fetch athlete from refs if needed (in parallel)
            if (athleteRefs.length > 0 && !athlete) {
              const athleteDetails = await fetchAthleteFromRef(athleteRefs[0]);
              if (athleteDetails) {
                athlete = athleteDetails;
              }
            }
            
            // Final fallback - create minimal athlete object
            if (!athlete) {
              console.warn(`No athlete data found for leader in category: ${categoryName}`, {
                leaderKeys: Object.keys(leader),
                leader: leader,
                categoryKeys: Object.keys(category),
              });
              athlete = {
                id: "",
                displayName: "Unknown Player",
                headshot: undefined,
                position: { abbreviation: "" },
              };
            }
            
            // Ensure athlete has all required fields
            const athleteData = {
              id: athlete.id || athlete.athleteId || "",
              displayName: athlete.displayName || athlete.name || athlete.fullName || "Unknown Player",
              headshot: athlete.headshot || athlete.image || athlete.photo || athlete.headshotUrl || athlete.headshot?.href,
              position: athlete.position || { 
                abbreviation: athlete.positionAbbr || athlete.position?.abbreviation || athlete.pos || "" 
              },
            };
            
            let statValue =
              leader.value ??
              leader.statValue ??
              leader.stat ??
              leader.statistics?.[0]?.value ??
              leader.stats?.[0]?.value ??
              0;
            let displayValue =
              leader.displayValue ||
              leader.displayStatValue ||
              leader.statistics?.[0]?.displayValue ||
              leader.stats?.[0]?.displayValue ||
              String(statValue);

            // Fetch stats in parallel if needed
            if (leader.statistics?.$ref && preferredStats.length > 0) {
              const statsData = await fetchStatsFromRef(leader.statistics.$ref);
              const extracted = extractStatFromDetails(statsData, preferredStats);
              if (extracted) {
                statValue = extracted.value;
                displayValue = extracted.displayValue;
              }
            }

            const leaderData = {
              athlete: athleteData,
              value: statValue,
              displayValue,
              statistics: leader.statistics || leader.stats || {},
            };

            return { leaderKey, leaderData };
          });

          // Wait for all leader processing to complete in parallel
          const leaderResults = await Promise.all(leaderPromises);
          
          // Map results to leaders object
          for (const result of leaderResults) {
            if (!result) continue;
            const { leaderKey, leaderData } = result;
            if (leaderKey === "passingYards") {
              leaders.passingYards = leaderData;
            } else if (leaderKey === "rushingYards") {
              leaders.rushingYards = leaderData;
            } else if (leaderKey === "receivingYards") {
              leaders.receivingYards = leaderData;
            } else if (leaderKey === "sacks") {
              leaders.sacks = leaderData;
            } else if (leaderKey === "tackles") {
              leaders.tackles = leaderData;
            }
          }

          return leaders;
        };

        // Initialize with basic data from prediction
        // gameDetails already fetched in parallel above
        let competition = null;
        let homeTeamId: string | undefined;
        let awayTeamId: string | undefined;

        try {

          // Extract team IDs from game details (most reliable source)
          competition = gameDetails?.competitions?.[0];
          
          const competitors = competition?.competitors || [];
          
          const homeCompetitor = competitors.find((c: any) => c.homeAway === "home");
          const awayCompetitor = competitors.find((c: any) => c.homeAway === "away");
          
          // Try multiple ways to get team ID
          homeTeamId = homeCompetitor?.team?.id || 
                      homeCompetitor?.teamId || 
                      homeCompetitor?.id ||
                      (homeCompetitor?.team?.$ref ? homeCompetitor.team.$ref.split('/').pop() : undefined);
          
          awayTeamId = awayCompetitor?.team?.id || 
                      awayCompetitor?.teamId || 
                      awayCompetitor?.id ||
                      (awayCompetitor?.team?.$ref ? awayCompetitor.team.$ref.split('/').pop() : undefined);
          
          // If still no IDs, try to get from team links/refs
          if (!homeTeamId && homeCompetitor?.team?.$ref) {
            const teamRef = homeCompetitor.team.$ref;
            const match = teamRef.match(/\/teams\/(\d+)/);
            if (match) homeTeamId = match[1];
          }
          
          if (!awayTeamId && awayCompetitor?.team?.$ref) {
            const teamRef = awayCompetitor.team.$ref;
            const match = teamRef.match(/\/teams\/(\d+)/);
            if (match) awayTeamId = match[1];
          }
          
          // If we still don't have team IDs, try to fetch them by team name
          if (!homeTeamId || !awayTeamId) {
            try {
              // Try to get team info by abbreviation/name
              const teamsUrl = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams`;
              const teamsResponse = await fetch(teamsUrl);
              if (teamsResponse.ok) {
                const teamsData = await teamsResponse.json();
                const allTeams = teamsData.sports?.[0]?.leagues?.[0]?.teams || [];
                
                if (!homeTeamId) {
                  const homeTeam = allTeams.find((t: any) => 
                    t.team?.abbreviation === prediction.home_team || 
                    t.team?.displayName?.includes(prediction.home_team)
                  );
                  if (homeTeam?.team?.id) {
                    homeTeamId = homeTeam.team.id;
                  }
                }
                
                if (!awayTeamId) {
                  const awayTeam = allTeams.find((t: any) => 
                    t.team?.abbreviation === prediction.away_team || 
                    t.team?.displayName?.includes(prediction.away_team)
                  );
                  if (awayTeam?.team?.id) {
                    awayTeamId = awayTeam.team.id;
                  }
                }
              }
            } catch (teamFetchError) {
              console.warn("Failed to fetch team IDs by name:", teamFetchError);
            }
          }
        } catch (err) {
          console.warn("Failed to fetch game details from ESPN, continuing with basic data:", err);
          // If we can't get team IDs from ESPN, we can't fetch leaders data
          console.warn("Cannot fetch team leaders without team IDs from ESPN");
        }

        const year = prediction.year;
        const seasonType = 2; // Regular season

        // Get match date from competition or gameDetails (competition already extracted above)
        const matchDate = competition?.date || gameDetails?.date;
        
        // Check if match date is in the past (previous match)
        let isMatchDateInPast = false;
        if (matchDate) {
          try {
            const matchDateObj = new Date(matchDate);
            const currentDate = new Date();
            // Normalize both dates to start of day for accurate comparison
            const matchDateNormalized = new Date(matchDateObj.getFullYear(), matchDateObj.getMonth(), matchDateObj.getDate());
            const currentDateNormalized = new Date(currentDate.getFullYear(), currentDate.getMonth(), currentDate.getDate());
            isMatchDateInPast = matchDateNormalized < currentDateNormalized;
          } catch (error) {
            console.warn("Failed to parse match date:", error);
          }
        }

        // Check if match is completed using multiple indicators:
        // 1. Database has scores
        // 2. ESPN API status indicates completion
        // 3. Match date is in the past (previous match)
        const hasDatabaseScores = prediction.home_score != null && prediction.away_score != null;
        
        // Also check ESPN API status for completion
        let isCompletedByESPN = false;
        let hasBoxscore = false;
        if (gameDetails) {
          hasBoxscore = competition?.boxscoreAvailable === true || gameDetails?.boxscoreAvailable === true;
          const hasRecap = competition?.recapAvailable === true || gameDetails?.recapAvailable === true;
          const hasWinner = competition?.competitors?.some((c: any) => c.winner === true || c.winner === false);
          
          const status = competition?.status || gameDetails.status;
          const statusType = status?.type || status;
          const statusCompleted = statusType?.completed || status?.completed;
          const statusName = statusType?.name || status?.name;
          const statusId = statusType?.id || status?.id;
          const statusState = statusType?.state || status?.state;
          
          isCompletedByESPN = 
            hasBoxscore ||
            (hasRecap && hasBoxscore) ||
            hasWinner ||
            statusCompleted === true ||
            statusName === "STATUS_FINAL" ||
            statusName === "STATUS_COMPLETE" ||
            statusName === "FINAL" ||
            statusName === "COMPLETE" ||
            statusId === 3 ||
            (statusId && statusId >= 3) ||
            statusState === "post" ||
            statusState === "final";
        }
        
        // Game is completed if:
        // - Database has scores, OR
        // - ESPN API says it's completed, OR
        // - Match date is in the past (previous match)
        // This ensures previous matches show game leaders and match stats
        const isMatchCompleted = hasDatabaseScores || isCompletedByESPN || isMatchDateInPast;
        const eventId = gameDetails?.id || matchId;
        
        // Only fetch game leaders if boxscore is available (leaders data requires boxscore)
        // This prevents unnecessary 404 requests in the network tab
        // For previous matches without boxscore, skip leaders entirely to avoid 404s
        // Be very conservative: 
        // - Match must be completed
        // - Boxscore must be confirmed available
        // - If match date is in past but no boxscore, definitely skip (prevents 404s)
        const shouldFetchGameLeaders = isMatchCompleted && hasBoxscore && gameDetails !== null && !(isMatchDateInPast && !hasBoxscore);


        // Fetch critical data first (leaders and team stats for completed matches)
        // This allows progressive loading - show basic info, then leaders, then non-critical data
        // Only fetch game leaders if boxscore is available to avoid unnecessary 404 requests
        const criticalDataPromises = [
          homeTeamId && eventId ? (shouldFetchGameLeaders
            ? getGameLeaders(eventId, homeTeamId).catch(err => {
                // Silently handle 404 errors - they're expected when leaders data isn't available
                const is404Error = err instanceof Error && err.message.includes('404') ||
                                  (err as any)?.status === 404 ||
                                  (err as any)?.response?.status === 404 ||
                                  String(err).includes('404');
                if (!is404Error) {
                  console.error(`[MatchDetails] Error fetching home game leaders:`, err);
                }
                return null;
              })
            : isMatchCompleted
            ? Promise.resolve(null) // Completed match but no boxscore - skip leaders
            : getTeamLeaders(year, seasonType, homeTeamId).catch(err => {
                console.error(`[MatchDetails] Error fetching home season leaders:`, err);
                return null;
              })
          ) : Promise.resolve(null),
          awayTeamId && eventId ? (shouldFetchGameLeaders
            ? getGameLeaders(eventId, awayTeamId).catch(err => {
                // Silently handle 404 errors - they're expected when leaders data isn't available
                const is404Error = err instanceof Error && err.message.includes('404') ||
                                  (err as any)?.status === 404 ||
                                  (err as any)?.response?.status === 404 ||
                                  String(err).includes('404');
                if (!is404Error) {
                  console.error(`[MatchDetails] Error fetching away game leaders:`, err);
                }
                return null;
              })
            : isMatchCompleted
            ? Promise.resolve(null) // Completed match but no boxscore - skip leaders
            : getTeamLeaders(year, seasonType, awayTeamId).catch(err => {
                console.error(`[MatchDetails] Error fetching away season leaders:`, err);
                return null;
              })
          ) : Promise.resolve(null),
          // Fetch team statistics only for completed matches
          isMatchCompleted && eventId ? getTeamStatistics(eventId).catch(err => {
            console.error(`Error fetching team statistics:`, err);
            return null;
          }) : Promise.resolve(null),
        ];

        const [homeLeadersResult, awayLeadersResult, teamStatsResult] = await Promise.allSettled(criticalDataPromises);

        // Fetch non-critical data (injuries, last 5 games) in parallel
        // For completed matches, we skip fetching these as they're not displayed
        // Only fetch if match is not completed (for upcoming matches)
        const nonCriticalPromises = isMatchCompleted ? [] : [
          homeTeamId ? getTeamEvents(year, homeTeamId).catch(err => {
            console.error(`Error fetching home events:`, err);
            return null;
          }) : Promise.resolve(null),
          awayTeamId ? getTeamEvents(year, awayTeamId).catch(err => {
            console.error(`Error fetching away events:`, err);
            return null;
          }) : Promise.resolve(null),
          homeTeamId ? getTeamInjuries(homeTeamId).catch(err => {
            console.error(`Error fetching home injuries:`, err);
            return { entries: [] };
          }) : Promise.resolve({ entries: [] }),
          awayTeamId ? getTeamInjuries(awayTeamId).catch(err => {
            console.error(`Error fetching away injuries:`, err);
            return { entries: [] };
          }) : Promise.resolve({ entries: [] }),
        ];

        const [homeEventsResult, awayEventsResult, homeInjuriesResult, awayInjuriesResult] = 
          nonCriticalPromises.length > 0 
            ? await Promise.allSettled(nonCriticalPromises)
            : [
                { status: 'fulfilled' as const, value: null },
                { status: 'fulfilled' as const, value: null },
                { status: 'fulfilled' as const, value: { entries: [] } },
                { status: 'fulfilled' as const, value: { entries: [] } },
              ];

        const homeLeaders = homeLeadersResult.status === 'fulfilled' ? homeLeadersResult.value : null;
        const awayLeaders = awayLeadersResult.status === 'fulfilled' ? awayLeadersResult.value : null;

        const homeEvents = homeEventsResult.status === 'fulfilled' ? homeEventsResult.value : null;
        const awayEvents = awayEventsResult.status === 'fulfilled' ? awayEventsResult.value : null;
        const homeInjuries = homeInjuriesResult.status === 'fulfilled' ? homeInjuriesResult.value : { entries: [] };
        const awayInjuries = awayInjuriesResult.status === 'fulfilled' ? awayInjuriesResult.value : { entries: [] };
        const teamStats = teamStatsResult.status === 'fulfilled' ? teamStatsResult.value : null;



        // Transform data for components
        const venue = competition?.venue;
        const date = competition?.date;

        // Extract data with safe fallbacks
        // Leaders data might be null, an object with items, or have a different structure
        const homeLeadersData = homeLeaders && typeof homeLeaders === 'object' && !Array.isArray(homeLeaders) 
          ? homeLeaders 
          : (Array.isArray(homeLeaders) ? { items: homeLeaders } : null);
        const awayLeadersData = awayLeaders && typeof awayLeaders === 'object' && !Array.isArray(awayLeaders)
          ? awayLeaders
          : (Array.isArray(awayLeaders) ? { items: awayLeaders } : null);
        
        
        // Events data - handle $ref links
        const extractEvents = (eventsData: any): any[] => {
          if (!eventsData) return [];
          if (Array.isArray(eventsData)) return eventsData;
          if (eventsData.items && Array.isArray(eventsData.items)) return eventsData.items;
          if (eventsData.events && Array.isArray(eventsData.events)) return eventsData.events;
          return [];
        };

        const homeEventsArray = extractEvents(homeEvents);
        const awayEventsArray = extractEvents(awayEvents);

        // Helper function to fetch event details from $ref
        const fetchEventFromRef = async (eventRef: string): Promise<any> => {
          try {
            const response = await fetch(ensureHttps(eventRef));
            if (response.ok) {
              return await response.json();
            }
          } catch (error) {
            console.warn(`Failed to fetch event from ${eventRef}:`, error);
          }
          return null;
        };

        // Helper function to transform games data
        const transformGames = async (events: any[], teamName: string, teamId: string): Promise<any[]> => {
          if (!events || events.length === 0) return [];


          // Fetch event details for any $ref links
          const eventsWithDetails = await Promise.all(
            events.map(async (event, index) => {
              // If event is just a $ref link, fetch the actual event data
              if (event.$ref && Object.keys(event).length === 1) {
                const eventData = await fetchEventFromRef(event.$ref);
                if (index < 3) {
                }
                return eventData || event;
              }
              return event;
            })
          );


          // Filter and sort completed games
          const sortedEvents = eventsWithDetails
            .filter((event, index) => {
              if (!event) return false;
              
              const competition = event.competitions?.[0];
              
              // Check if game is completed using multiple indicators:
              // 1. boxscoreAvailable and recapAvailable indicate game is finished
              // 2. winner property on competitors indicates game has been played
              // 3. Check status if available
              const hasBoxscore = competition?.boxscoreAvailable === true;
              const hasRecap = competition?.recapAvailable === true;
              const hasWinner = competition?.competitors?.some((c: any) => c.winner === true || c.winner === false);
              
              // Also check status if it exists
              const status = competition?.status || event.status;
              const statusType = status?.type || status;
              const statusCompleted = statusType?.completed || status?.completed;
              const statusName = statusType?.name || status?.name;
              const statusId = statusType?.id || status?.id;
              const statusState = statusType?.state || status?.state;
              
              // Game is completed if any of these conditions are true
              const isCompleted = 
                hasBoxscore || // Has boxscore = game finished
                (hasRecap && hasBoxscore) || // Has recap and boxscore = definitely finished
                hasWinner || // Has winner/loser = game played
                statusCompleted === true ||
                statusName === "STATUS_FINAL" ||
                statusName === "STATUS_COMPLETE" ||
                statusName === "FINAL" ||
                statusName === "COMPLETE" ||
                statusId === 3 ||
                (statusId && statusId >= 3) ||
                statusState === "post" ||
                statusState === "final";
              
              return isCompleted;
            })
            .sort((a, b) => {
              const dateA = new Date(a.date || a.competitions?.[0]?.date || 0).getTime();
              const dateB = new Date(b.date || b.competitions?.[0]?.date || 0).getTime();
              return dateB - dateA; // Most recent first
            })
            .slice(0, 5); // Take last 5 games


          // Helper to fetch score from $ref if needed
          const fetchScoreFromRef = async (scoreRef: string | number | object): Promise<number> => {
            if (typeof scoreRef === 'number') return scoreRef;
            if (typeof scoreRef === 'string' && !scoreRef.includes('http')) {
              const parsed = parseInt(scoreRef, 10);
              if (!isNaN(parsed)) return parsed;
            }
            if (typeof scoreRef === 'object' && scoreRef !== null && '$ref' in scoreRef) {
              try {
                const response = await fetch(ensureHttps((scoreRef as any).$ref));
                if (response.ok) {
                  const scoreData = await response.json();
                  return parseInt(String(scoreData.value || scoreData.displayValue || scoreData || 0), 10);
                }
              } catch (error) {
                console.warn(`Failed to fetch score from ${(scoreRef as any).$ref}:`, error);
              }
            }
            return 0;
          };

          // Transform games - need to await score fetches
          const transformedGames = await Promise.all(
            sortedEvents.map(async (event) => {
              const competition = event.competitions?.[0];
              const competitors = competition?.competitors || [];
              
              // Find team and opponent
              const teamCompetitor = competitors.find(
                (c: any) => {
                  const cTeamId = c.team?.id || c.team?.$ref?.match(/\/teams\/(\d+)/)?.[1];
                  const cTeamName = c.team?.displayName || c.team?.name || c.team?.abbreviation;
                  return cTeamId === teamId || 
                         cTeamName === teamName ||
                         cTeamName?.includes(teamName) ||
                         teamName?.includes(cTeamName);
                }
              );
              const opponentCompetitor = competitors.find(
                (c: any) => {
                  const cTeamId = c.team?.id || c.team?.$ref?.match(/\/teams\/(\d+)/)?.[1];
                  const cTeamName = c.team?.displayName || c.team?.name || c.team?.abbreviation;
                  return cTeamId !== teamId && 
                         cTeamName !== teamName &&
                         !cTeamName?.includes(teamName) &&
                         !teamName?.includes(cTeamName);
                }
              );

              // Extract scores - handle both direct values and $ref links
              let teamScore = 0;
              let opponentScore = 0;
              
              if (teamCompetitor?.score) {
                teamScore = await fetchScoreFromRef(teamCompetitor.score);
              } else if (teamCompetitor?.linescores && Array.isArray(teamCompetitor.linescores)) {
                teamScore = teamCompetitor.linescores.reduce((sum: number, line: any) => 
                  sum + (parseInt(String(line.value || line.displayValue || 0), 10)), 0);
              }
              
              if (opponentCompetitor?.score) {
                opponentScore = await fetchScoreFromRef(opponentCompetitor.score);
              } else if (opponentCompetitor?.linescores && Array.isArray(opponentCompetitor.linescores)) {
                opponentScore = opponentCompetitor.linescores.reduce((sum: number, line: any) => 
                  sum + (parseInt(String(line.value || line.displayValue || 0), 10)), 0);
              }

              // Determine result
              let result: "W" | "L" | "T" = "T";
              if (teamScore > opponentScore) result = "W";
              else if (teamScore < opponentScore) result = "L";

              // Get opponent info - handle $ref links for team data
              let opponentName = "Unknown";
              let opponentLogo: string | undefined;
              
              if (opponentCompetitor?.team) {
                if (opponentCompetitor.team.$ref) {
                  // Fetch team data from $ref
                  try {
                    const teamResponse = await fetch(ensureHttps(opponentCompetitor.team.$ref));
                    if (teamResponse.ok) {
                      const teamData = await teamResponse.json();
                      opponentName = teamData.displayName || teamData.name || teamData.abbreviation || "Unknown";
                      opponentLogo = teamData.logo || teamData.logos?.[0]?.href;
                    }
                  } catch (error) {
                    console.warn(`Failed to fetch opponent team data:`, error);
                  }
                } else {
                  opponentName = opponentCompetitor.team.displayName || 
                                opponentCompetitor.team.name || 
                                opponentCompetitor.team.abbreviation ||
                                "Unknown";
                  opponentLogo = opponentCompetitor.team.logo || 
                                opponentCompetitor.team.logos?.[0]?.href;
                }
              }

              // Determine if team was home or away
              const isHome = teamCompetitor?.homeAway === "home";

              return {
                id: event.id || event.$ref?.match(/\/events\/(\d+)/)?.[1] || `event-${Date.now()}`,
                date: event.date || competition?.date || event.competitions?.[0]?.date,
                opponent: {
                  name: opponentName,
                  logo: opponentLogo,
                  isHome: !isHome, // If team was home, opponent was away (and vice versa)
                },
                score: {
                  team: teamScore,
                  opponent: opponentScore,
                },
                result,
              };
            })
          );
          
          return transformedGames;
        };
        
        // Transform injuries data
        const transformInjuries = async (injuriesData: any): Promise<any[]> => {
          if (!injuriesData) {
            return [];
          }

          // ESPN API can return injuries in different structures
          let entries: any[] = [];

          // Structure 1: entries array directly
          if (Array.isArray(injuriesData.entries)) {
            entries = injuriesData.entries;
          }
          // Structure 2: injuries array
          else if (Array.isArray(injuriesData.injuries)) {
            entries = injuriesData.injuries;
          }
          // Structure 3: items array (from core API v2 endpoint)
          else if (Array.isArray(injuriesData.items)) {
            entries = injuriesData.items;
          }
          // Structure 4: data.entries
          else if (Array.isArray(injuriesData.data?.entries)) {
            entries = injuriesData.data.entries;
          }
          // Structure 5: injuriesData is already an array
          else if (Array.isArray(injuriesData)) {
            entries = injuriesData;
          }
          // Structure 6: team.injuries
          else if (Array.isArray(injuriesData.team?.injuries)) {
            entries = injuriesData.team.injuries;
          }

          // Flatten nested arrays (in case entries contain arrays)
          entries = entries.flat();

          if (entries.length === 0) {
            return [];
          }

          // Helper function to fetch injury data from $ref
          const fetchInjuryFromRef = async (injuryRef: string): Promise<any> => {
            try {
              const response = await fetch(ensureHttps(injuryRef));
              if (response.ok) {
                const data = await response.json();
                return data;
              }
            } catch (error) {
              console.warn(`Failed to fetch injury from ${injuryRef}:`, error);
            }
            return null;
          };

          // Transform each injury entry
          const transformedInjuries = await Promise.all(
            entries.map(async (entry: any, index: number) => {
              // Handle case where entry might be an array itself
              let actualEntry = Array.isArray(entry) ? entry[0] : entry;
              
              if (!actualEntry || typeof actualEntry !== 'object') {
                console.warn(`Skipping invalid entry at index ${index}:`, entry);
                return null;
              }

              // If the entry is just a $ref, fetch the actual injury data
              if (actualEntry.$ref && Object.keys(actualEntry).length === 1) {
                const injuryData = await fetchInjuryFromRef(actualEntry.$ref);
                if (injuryData) {
                  actualEntry = injuryData;
                } else {
                  console.warn(`Failed to fetch injury data for entry at index ${index}`);
                  return null;
                }
              }

              // Extract athlete information
              let athlete = null;

              // Try different athlete locations
              if (actualEntry.athlete) {
                if (typeof actualEntry.athlete === 'object' && !actualEntry.athlete.$ref) {
                  athlete = actualEntry.athlete;
                } else if (actualEntry.athlete.$ref) {
                  // Fetch athlete from $ref
                  const athleteDetails = await fetchAthleteFromRef(actualEntry.athlete.$ref);
                  if (athleteDetails) {
                    athlete = athleteDetails;
                  }
                }
              }

              // Try player object
              if (!athlete && actualEntry.player) {
                athlete = actualEntry.player;
              }

              // Try extracting athlete ID from the injury $ref URL if we have it
              if (!athlete && actualEntry.$ref) {
                const athleteIdMatch = actualEntry.$ref.match(/\/athletes\/(\d+)\//);
                if (athleteIdMatch) {
                  const athleteId = athleteIdMatch[1];
                  // Try to fetch athlete info using the ID from the URL
                  const athleteUrl = `https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2025/athletes/${athleteId}?lang=en&region=us`;
                  const athleteDetails = await fetchAthleteFromRef(athleteUrl);
                  if (athleteDetails) {
                    athlete = athleteDetails;
                  }
                }
              }

              // Fallback - create athlete from entry data
              if (!athlete) {
                // Try to extract athlete ID from the original entry $ref if available
                let athleteId = actualEntry.id || actualEntry.athleteId;
                const originalEntry = Array.isArray(entry) ? entry[0] : entry;
                if (!athleteId && originalEntry?.$ref) {
                  const match = originalEntry.$ref.match(/\/athletes\/(\d+)\//);
                  if (match) {
                    athleteId = match[1];
                  }
                }
                
                // Normalize position to only include abbreviation
                let positionAbbr = "";
                if (actualEntry.position) {
                  if (typeof actualEntry.position === 'object') {
                    positionAbbr = actualEntry.position.abbreviation || actualEntry.positionAbbr || "";
                  } else {
                    positionAbbr = actualEntry.position;
                  }
                } else {
                  positionAbbr = actualEntry.positionAbbr || "";
                }
                
                athlete = {
                  id: athleteId || `unknown-${index}`,
                  displayName: actualEntry.displayName || actualEntry.name || actualEntry.fullName || "Unknown Player",
                  headshot: actualEntry.headshot || actualEntry.image || actualEntry.photo,
                  position: { abbreviation: positionAbbr },
                };
              }

              // Extract status - ESPN API can have status in different places
              let status: { id: string; name: string; description: string; displayName?: string } | null = null;
              if (actualEntry.status) {
                if (typeof actualEntry.status === 'object' && actualEntry.status !== null) {
                  // Ensure all properties are strings
                  status = {
                    id: typeof actualEntry.status.id === 'string' ? actualEntry.status.id :
                         typeof actualEntry.status.id === 'number' ? String(actualEntry.status.id) : "",
                    name: typeof actualEntry.status.name === 'string' ? actualEntry.status.name :
                          typeof actualEntry.status.displayName === 'string' ? actualEntry.status.displayName : "Unknown",
                    description: typeof actualEntry.status.description === 'string' ? actualEntry.status.description : "",
                  };
                } else if (typeof actualEntry.status === 'string') {
                  status = { id: "", name: actualEntry.status, description: "" };
                }
              }
              
              if (!status) {
                // Try to extract from other fields
                const statusName = typeof actualEntry.statusName === 'string' ? actualEntry.statusName :
                                  typeof actualEntry.statusType === 'string' ? actualEntry.statusType : "Unknown";
                const statusId = typeof actualEntry.statusId === 'string' ? actualEntry.statusId :
                                typeof actualEntry.statusId === 'number' ? String(actualEntry.statusId) : "";
                const statusDesc = typeof actualEntry.statusDescription === 'string' ? actualEntry.statusDescription : "";
                
                status = {
                  id: statusId,
                  name: statusName,
                  description: statusDesc,
                };
              }

              // Extract injury info
              let injury = null;
              if (actualEntry.injury) {
                if (typeof actualEntry.injury === 'object') {
                  injury = actualEntry.injury;
                } else {
                  injury = { type: actualEntry.injury, date: actualEntry.injuryDate };
                }
              } else if (actualEntry.injuryType || actualEntry.type) {
                injury = {
                  type: actualEntry.injuryType || actualEntry.type || "",
                  date: actualEntry.injuryDate || actualEntry.date,
                };
              }

              // Extract estimated return date
              const estimatedReturn = actualEntry.estimatedReturn || 
                                    actualEntry.returnDate || 
                                    actualEntry.expectedReturn ||
                                    actualEntry.date ||
                                    actualEntry.return;

              // Ensure athlete.id is never empty to avoid duplicate keys
              const athleteId = athlete.id || athlete.athleteId || `injury-${index}`;

              // Normalize position to only include abbreviation (avoid rendering full object)
              let positionAbbr = "";
              if (athlete.position) {
                if (typeof athlete.position === 'object') {
                  const abbr = athlete.position.abbreviation || athlete.positionAbbr;
                  positionAbbr = typeof abbr === 'string' ? abbr : "";
                } else if (typeof athlete.position === 'string') {
                  positionAbbr = athlete.position;
                }
              } else {
                const abbr = athlete.positionAbbr;
                positionAbbr = typeof abbr === 'string' ? abbr : "";
              }

              // Ensure status.name is always a string
              let statusName = "Unknown";
              if (status) {
                if (typeof status.name === 'string') {
                  statusName = status.name;
                } else if (typeof status.displayName === 'string') {
                  statusName = status.displayName;
                } else if (typeof status === 'string') {
                  statusName = status;
                }
              }

              // Ensure status.id and status.description are strings
              const statusId = typeof status?.id === 'string' ? status.id : 
                              typeof status?.id === 'number' ? String(status.id) : "";
              const statusDesc = typeof status?.description === 'string' ? status.description : "";

              // Ensure displayName is always a string
              const displayName = typeof athlete.displayName === 'string' ? athlete.displayName :
                                 typeof athlete.name === 'string' ? athlete.name :
                                 typeof athlete.fullName === 'string' ? athlete.fullName :
                                 "Unknown Player";

              return {
                athlete: {
                  id: athleteId,
                  displayName: displayName,
                  headshot: typeof athlete.headshot === 'string' ? athlete.headshot :
                           typeof athlete.image === 'string' ? athlete.image :
                           typeof athlete.photo === 'string' ? athlete.photo :
                           typeof athlete.headshot?.href === 'string' ? athlete.headshot.href :
                           undefined,
                  position: { abbreviation: positionAbbr },
                },
                status: {
                  id: statusId,
                  name: statusName,
                  description: statusDesc,
                },
                injury: injury && typeof injury.type === 'string' ? { type: injury.type, date: typeof injury.date === 'string' ? injury.date : undefined } : undefined,
                practiceStatus: typeof actualEntry.practiceStatus === 'string' ? actualEntry.practiceStatus :
                               typeof actualEntry.practice === 'string' ? actualEntry.practice :
                               typeof actualEntry.practiceStatusName === 'string' ? actualEntry.practiceStatusName :
                               "",
                estimatedReturn: typeof estimatedReturn === 'string' ? estimatedReturn : undefined,
              };
            })
          );

          // Filter out null entries
          const validInjuries = transformedInjuries.filter((injury): injury is any => injury !== null);

          return validInjuries;
        };

        // Transform leaders data in parallel
        const [homeLeadersTransformed, awayLeadersTransformed] = await Promise.all([
          transformLeaders(homeLeadersData),
          transformLeaders(awayLeadersData),
        ]);

        // Transform injuries data (only if not completed match)
        const [homeInjuriesTransformed, awayInjuriesTransformed] = isMatchCompleted 
          ? [[], []]
          : await Promise.all([
              transformInjuries(homeInjuries),
              transformInjuries(awayInjuries),
            ]);

        // Transform games data in parallel (only if not completed match)
        const [homeGamesTransformed, awayGamesTransformed] = isMatchCompleted
          ? [[], []]
          : await Promise.all([
              transformGames(homeEventsArray, prediction.home_team, homeTeamId),
              transformGames(awayEventsArray, prediction.away_team, awayTeamId),
            ]);

        // Extract game time from the date
        let gameTime: string | undefined = undefined;
        if (date) {
          try {
            const dateObj = new Date(date);
            if (!isNaN(dateObj.getTime())) {
              // Format time in 12-hour format with AM/PM
              gameTime = dateObj.toLocaleTimeString("en-US", { 
                hour: "numeric", 
                minute: "2-digit",
                hour12: true 
              });
            }
          } catch (error) {
            console.warn("Failed to parse game time from date:", error);
          }
        }

        const detailsData: MatchDetailsData = {
          gameInfo: {
            date: date,
            time: gameTime,
            stadium: venue?.fullName || prediction.stadium || "Stadium TBA",
            location: venue?.address
              ? `${venue.address.city}, ${venue.address.state}`
              : undefined,
            weather: {
              temperature: venue?.weather?.temperature,
              condition: venue?.weather?.displayValue,
              source: venue?.weather?.link,
            },
            stadiumImage: venue?.images?.[0]?.href,
          },
          seasonLeaders: {
            homeTeam: {
              name: prediction.home_team,
              logo: prediction.home_team_image_url,
              leaders: homeLeadersTransformed,
            },
            awayTeam: {
              name: prediction.away_team,
              logo: prediction.away_team_image_url,
              leaders: awayLeadersTransformed,
            },
            isGameLeaders: isMatchCompleted, // true if match is completed
          },
          injuries: {
            homeTeam: {
              name: prediction.home_team,
              logo: prediction.home_team_image_url,
              injuries: homeInjuriesTransformed,
            },
            awayTeam: {
              name: prediction.away_team,
              logo: prediction.away_team_image_url,
              injuries: awayInjuriesTransformed,
            },
          },
          last5Games: {
            homeTeam: {
              name: prediction.home_team,
              logo: prediction.home_team_image_url,
              games: homeGamesTransformed,
            },
            awayTeam: {
              name: prediction.away_team,
              logo: prediction.away_team_image_url,
              games: awayGamesTransformed,
            },
          },
          // Only include team stats for completed matches
          teamStats: teamStats ? {
            homeTeam: {
              name: teamStats.homeTeam.name || prediction.home_team,
              logo: teamStats.homeTeam.logo || prediction.home_team_image_url,
              abbreviation: teamStats.homeTeam.abbreviation,
              totalYards: teamStats.homeTeam.totalYards,
              turnovers: teamStats.homeTeam.turnovers,
              firstDowns: teamStats.homeTeam.firstDowns,
              penalties: teamStats.homeTeam.penalties,
              thirdDown: teamStats.homeTeam.thirdDown,
              fourthDown: teamStats.homeTeam.fourthDown,
              redZone: teamStats.homeTeam.redZone,
              possession: teamStats.homeTeam.possession,
            },
            awayTeam: {
              name: teamStats.awayTeam.name || prediction.away_team,
              logo: teamStats.awayTeam.logo || prediction.away_team_image_url,
              abbreviation: teamStats.awayTeam.abbreviation,
              totalYards: teamStats.awayTeam.totalYards,
              turnovers: teamStats.awayTeam.turnovers,
              firstDowns: teamStats.awayTeam.firstDowns,
              penalties: teamStats.awayTeam.penalties,
              thirdDown: teamStats.awayTeam.thirdDown,
              fourthDown: teamStats.awayTeam.fourthDown,
              redZone: teamStats.awayTeam.redZone,
              possession: teamStats.awayTeam.possession,
            },
          } : undefined,
        };

        if (detailsData.last5Games.homeTeam.games.length > 0) {
        }
        if (detailsData.last5Games.awayTeam.games.length > 0) {
        }

        setDetails(detailsData);
        setLoading(false); // Set loading to false after details are ready
      } catch (error) {
        console.error("Error fetching match details:", error);
        const errorMessage = error instanceof Error ? error.message : "Failed to load match details";
        setError(errorMessage);
        // Don't show toast for every error, just log it
        console.error("Match details error:", errorMessage);
        // Still set loading to false so page can render with basic data
        setLoading(false);
      }
    };

    if (matchId) {
      fetchMatchDetails();
    } else {
      setError("Match ID is required");
      setLoading(false);
    }
  }, [matchId, navigate, toast]);

  if (loading) {
    return (
      <div className="min-h-screen bg-background">
        <Header />
        <div className="flex items-center justify-center min-h-[60vh]">
          <div className="flex flex-col items-center gap-4">
            <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
            <p className="text-muted-foreground">Loading match details...</p>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-background">
        <Header />
        <div className="max-w-7xl mx-auto px-4 py-8">
          <Button variant="ghost" onClick={navigateBack} className="mb-4">
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Matches
          </Button>
          <div className="text-center py-12">
            <p className="text-destructive text-lg font-semibold mb-2">Error loading match details</p>
            <p className="text-muted-foreground">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  // Ensure we have at least basic match data
  if (!matchData) {
    return (
      <div className="min-h-screen bg-background">
        <Header />
        <div className="max-w-7xl mx-auto px-4 py-8">
          <Button variant="ghost" onClick={navigateBack} className="mb-4">
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Matches
          </Button>
          <div className="text-center py-12">
            <p className="text-muted-foreground text-lg">Loading match data...</p>
          </div>
        </div>
      </div>
    );
  }

  // Create default details if not available yet
  const defaultDetails: MatchDetailsData = {
    gameInfo: {
      date: undefined,
      time: undefined,
      stadium: matchData.stadium || "Stadium TBA",
      location: undefined,
      weather: undefined,
      stadiumImage: undefined,
    },
    seasonLeaders: {
      homeTeam: {
        name: matchData.home_team,
        logo: matchData.home_team_image_url,
        leaders: {},
      },
      awayTeam: {
        name: matchData.away_team,
        logo: matchData.away_team_image_url,
        leaders: {},
      },
      isGameLeaders: false,
    },
    injuries: {
      homeTeam: {
        name: matchData.home_team,
        logo: matchData.home_team_image_url,
        injuries: [],
      },
      awayTeam: {
        name: matchData.away_team,
        logo: matchData.away_team_image_url,
        injuries: [],
      },
    },
    last5Games: {
      homeTeam: {
        name: matchData.home_team,
        logo: matchData.home_team_image_url,
        games: [],
      },
      awayTeam: {
        name: matchData.away_team,
        logo: matchData.away_team_image_url,
        games: [],
      },
    },
  };

  const displayDetails = details || defaultDetails;

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <div className="max-w-7xl mx-auto px-4 py-8">
        <Button variant="ghost" onClick={navigateBack} className="mb-6">
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Matches
        </Button>

        <div className="space-y-6">
          <GameInformation {...displayDetails.gameInfo} />
          {displayDetails.teamStats && (
            <TeamStats
              homeTeam={{
                name: displayDetails.teamStats.homeTeam.name,
                logo: displayDetails.teamStats.homeTeam.logo,
                abbreviation: displayDetails.teamStats.homeTeam.abbreviation,
              }}
              awayTeam={{
                name: displayDetails.teamStats.awayTeam.name,
                logo: displayDetails.teamStats.awayTeam.logo,
                abbreviation: displayDetails.teamStats.awayTeam.abbreviation,
              }}
              stats={{
                totalYards: displayDetails.teamStats.homeTeam.totalYards != null && displayDetails.teamStats.awayTeam.totalYards != null
                  ? { home: displayDetails.teamStats.homeTeam.totalYards, away: displayDetails.teamStats.awayTeam.totalYards }
                  : undefined,
                turnovers: displayDetails.teamStats.homeTeam.turnovers != null && displayDetails.teamStats.awayTeam.turnovers != null
                  ? { home: displayDetails.teamStats.homeTeam.turnovers, away: displayDetails.teamStats.awayTeam.turnovers }
                  : undefined,
                firstDowns: displayDetails.teamStats.homeTeam.firstDowns != null && displayDetails.teamStats.awayTeam.firstDowns != null
                  ? { home: displayDetails.teamStats.homeTeam.firstDowns, away: displayDetails.teamStats.awayTeam.firstDowns }
                  : undefined,
                penalties: displayDetails.teamStats.homeTeam.penalties && displayDetails.teamStats.awayTeam.penalties
                  ? { home: displayDetails.teamStats.homeTeam.penalties, away: displayDetails.teamStats.awayTeam.penalties }
                  : undefined,
                thirdDown: displayDetails.teamStats.homeTeam.thirdDown && displayDetails.teamStats.awayTeam.thirdDown
                  ? { home: displayDetails.teamStats.homeTeam.thirdDown, away: displayDetails.teamStats.awayTeam.thirdDown }
                  : undefined,
                fourthDown: displayDetails.teamStats.homeTeam.fourthDown && displayDetails.teamStats.awayTeam.fourthDown
                  ? { home: displayDetails.teamStats.homeTeam.fourthDown, away: displayDetails.teamStats.awayTeam.fourthDown }
                  : undefined,
                redZone: displayDetails.teamStats.homeTeam.redZone && displayDetails.teamStats.awayTeam.redZone
                  ? { home: displayDetails.teamStats.homeTeam.redZone, away: displayDetails.teamStats.awayTeam.redZone }
                  : undefined,
                possession: displayDetails.teamStats.homeTeam.possession && displayDetails.teamStats.awayTeam.possession
                  ? { home: displayDetails.teamStats.homeTeam.possession, away: displayDetails.teamStats.awayTeam.possession }
                  : undefined,
              }}
            />
          )}
          <SeasonLeaders
            {...displayDetails.seasonLeaders}
            isGameLeaders={displayDetails.seasonLeaders.isGameLeaders}
          />
          {/* Only show Injury Report and Last 5 Games for upcoming matches (not completed) */}
          {/* Hide if game is completed (has game leaders or team stats) */}
          {!displayDetails.seasonLeaders.isGameLeaders && !displayDetails.teamStats && (
            <>
              <InjuryReport {...displayDetails.injuries} />
              <Last5Games {...displayDetails.last5Games} />
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default MatchDetails;

