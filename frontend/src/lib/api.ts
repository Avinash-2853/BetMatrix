const DEFAULT_API_BASE_URL = "http://localhost:8000/api";

/**
 * Get the API base URL from environment variables.
 * Note: Vite requires the VITE_ prefix for environment variables to be accessible in the frontend.
 * Set VITE_API_BASE_URL in your .env file (e.g., VITE_API_BASE_URL=http://localhost:8000/api)
 */
const getApiBaseUrl = (): string => {
  const envUrl = import.meta.env.VITE_API_BASE_URL;
  
  if (envUrl) {
    // Remove trailing slashes
    return envUrl.replace(/\/+$/, "");
  }
  
  return DEFAULT_API_BASE_URL;
};

export const API_BASE_URL = getApiBaseUrl();

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

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || response.statusText);
  }

  return response.json() as Promise<T>;
}

export interface PredictionResponse {
  game_id: string;
  year: number;
  week: number;
  home_team: string;
  away_team: string;
  home_team_win_probability: number;
  away_team_win_probability: number;
  predicted_result: string;
  home_team_image_url: string;
  away_team_image_url: string;
  home_coach: string;
  away_coach: string;
  stadium: string;
  home_score?: number | null;
  away_score?: number | null;
}

export interface GameMetadata {
  date: string | null;
  homeScore: number | null;
  awayScore: number | null;
  statusText: string | null;
  state: string | null;
  isCompleted: boolean;
}

export interface PredictionsListResponse {
  predictions: PredictionResponse[];
  total_count: number;
  year: number;
  week: number;
}

export interface AvailableDataResponse {
  years: number[];
  weeks: number[];
}

export const getAvailableData = () =>
  fetchJson<AvailableDataResponse>("/fetch-data/available-data");

export const getPredictions = (year: number, week: number) =>
  fetchJson<PredictionsListResponse>(`/fetch-data/predictions?year=${year}&week=${week}`);

export const getPredictionByGameId = (gameId: string) =>
  fetchJson<PredictionResponse>(`/fetch-data/predictions/${gameId}`);

/**
 * Fetch NFL schedule data from ESPN API to get accurate week date ranges
 */
export const getNFLSchedule = async (year: number, week: number) => {
  const url = `https://cdn.espn.com/core/nfl/schedule?xhr=1&year=${year}&week=${week}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch schedule: ${response.statusText}`);
  }
  return response.json();
};

/**
 * Fetch game date for a specific game ID from ESPN API
 */
export const getGameDate = async (gameId: string): Promise<string | null> => {
  try {
    const url = `https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/${gameId}`;
    const response = await fetch(url);
    if (!response.ok) {
      return null;
    }
    const data = await response.json();
    const competitions = data?.competitions?.[0];
    if (competitions?.date) {
      return competitions.date;
    }
    return null;
  } catch (error) {
    console.error(`Error fetching game date for ${gameId}:`, error);
    return null;
  }
};

const parseScoreValue = (value: unknown): number | null => {
  if (value == null) return null;
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (typeof value === "object") {
    const obj = value as { value?: unknown; displayValue?: unknown };
    if (obj.value != null) {
      return parseScoreValue(obj.value);
    }
    if (obj.displayValue != null) {
      return parseScoreValue(obj.displayValue);
    }
  }
  return null;
};

/**
 * Fetch match results (scores) from ESPN API
 * This is the primary source of truth for match results
 * Endpoint: https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{eventId}
 */
export const getGameMetadata = async (eventId: string): Promise<GameMetadata | null> => {
  try {
    const data = await getGameDetails(eventId);
    let competition = data?.competitions?.[0];
    
    // If competition is a $ref, fetch it
    if (competition?.$ref && Object.keys(competition).length === 1) {
      try {
        const compResponse = await fetch(ensureHttps(competition.$ref));
        if (compResponse.ok) {
          competition = await compResponse.json();
        }
      } catch (error) {
        console.warn(`Failed to fetch competition from $ref:`, error);
      }
    }
    
    const statusType = competition?.status?.type;
    const state = statusType?.state ?? null;
    const statusId = statusType?.id;
    
    // Check if match is completed - try multiple indicators
    const isCompleted =
      Boolean(statusType?.completed) ||
      (typeof state === "string" && state.toLowerCase() === "post") ||
      statusId === 3 || // FINAL status ID
      statusType?.name === "STATUS_FINAL" ||
      statusType?.description === "Final" ||
      (statusType?.shortDetail && statusType.shortDetail.toLowerCase().includes("final"));
    
    const statusText =
      statusType?.shortDetail ||
      statusType?.detail ||
      statusType?.description ||
      statusType?.name ||
      (state ? state.toUpperCase() : null);

    let competitors = competition?.competitors ?? [];
    
    // If competitors are $ref links, fetch them
    if (competitors.length > 0 && competitors[0]?.$ref && Object.keys(competitors[0]).length === 1) {
      try {
        const competitorsPromises = competitors.map(async (comp: any) => {
          if (comp.$ref) {
            const compResponse = await fetch(ensureHttps(comp.$ref));
            if (compResponse.ok) {
              return await compResponse.json();
            }
          }
          return comp;
        });
        competitors = await Promise.all(competitorsPromises);
      } catch (error) {
        console.warn(`Failed to fetch competitors from $ref:`, error);
      }
    }
    let homeScore: number | null = null;
    let awayScore: number | null = null;

    // Extract scores from ESPN API response
    // ESPN API may return scores directly or as $ref links that need to be fetched
    const scorePromises = competitors.map(async (competitor: any) => {
      let scoreValue: number | null = null;
      
      // Try direct score value first (score object might have value embedded even with $ref)
      if (competitor?.score?.value != null) {
        scoreValue = parseScoreValue(competitor.score.value);
      } else if (competitor?.score && typeof competitor.score === 'number') {
        scoreValue = parseScoreValue(competitor.score);
      } else if (competitor?.score?.$ref) {
        // Fetch score from $ref link
        try {
          const scoreResponse = await fetch(ensureHttps(competitor.score.$ref));
          if (scoreResponse.ok) {
            const scoreData = await scoreResponse.json();
            // The $ref response might have value directly or nested
            scoreValue = parseScoreValue(
              scoreData?.value ?? 
              scoreData?.displayValue ??
              scoreData?.score?.value ??
              scoreData?.score?.displayValue
            );
          }
        } catch (error) {
          console.warn(`Failed to fetch score from $ref for ${competitor.homeAway}:`, error);
        }
      }
      
      // Fallback: try linescores
      if (scoreValue == null) {
        if (Array.isArray(competitor?.linescores) && competitor.linescores.length > 0) {
          // Sum all quarter scores
          const totalScore = competitor.linescores.reduce((sum: number, q: any) => {
            const qScore = parseScoreValue(q?.value ?? q?.displayValue);
            return sum + (qScore ?? 0);
          }, 0);
          scoreValue = totalScore > 0 ? totalScore : null;
        } else if (competitor?.linescores?.$ref) {
          try {
            const linescoresResponse = await fetch(ensureHttps(competitor.linescores.$ref));
            if (linescoresResponse.ok) {
              const linescoresData = await linescoresResponse.json();
              if (Array.isArray(linescoresData?.items)) {
                const totalScore = linescoresData.items.reduce((sum: number, q: any) => {
                  const qScore = parseScoreValue(q?.value ?? q?.displayValue);
                  return sum + (qScore ?? 0);
                }, 0);
                scoreValue = totalScore > 0 ? totalScore : null;
              }
            }
          } catch (error) {
            console.warn(`Failed to fetch linescores from $ref for ${competitor.homeAway}:`, error);
          }
        }
      }
      
      return {
        homeAway: competitor?.homeAway,
        score: scoreValue,
      };
    });
    
    const scoreResults = await Promise.all(scorePromises);
    scoreResults.forEach((result) => {
      if (result.homeAway === "home") {
        homeScore = result.score;
      } else if (result.homeAway === "away") {
        awayScore = result.score;
      }
    });
    
    // Debug logging for score extraction
    if (isCompleted && (homeScore == null || awayScore == null)) {
      console.warn(`[getGameMetadata] Completed match ${eventId} missing scores:`, {
        homeScore,
        awayScore,
        competitors: competitors.map((c: any) => ({
          homeAway: c?.homeAway,
          score: c?.score,
          hasScoreRef: !!c?.score?.$ref,
        })),
      });
    } else if (homeScore != null && awayScore != null) {
    }

    return {
      date: competition?.date ?? data?.date ?? null,
      homeScore,
      awayScore,
      statusText: isCompleted ? (statusText ?? "FINAL") : null,
      state,
      isCompleted,
    };
  } catch (error) {
    console.error(`Error fetching match results from ESPN API for event ${eventId}:`, error);
    return null;
  }
};

/**
 * Fetch detailed game information from ESPN API
 */
export const getGameDetails = async (eventId: string) => {
  try {
    const url = `https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/${eventId}`;
    const response = await fetch(url);
    
    if (!response.ok) {
      console.error(`Game details response not OK: ${response.status} ${response.statusText}`);
      throw new Error(`Failed to fetch game details: ${response.statusText}`);
    }
    const data = await response.json();
    return data;
  } catch (error) {
    console.error(`Error fetching game details for ${eventId}:`, error);
    throw error;
  }
};

/**
 * Fetch team information from ESPN API
 */
export const getTeamInfo = async (teamId: string) => {
  try {
    const url = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/${teamId}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch team info: ${response.statusText}`);
    }
    return response.json();
  } catch (error) {
    console.error(`Error fetching team info for ${teamId}:`, error);
    throw error;
  }
};

/**
 * Fetch season leaders for a team from ESPN API
 * Tries multiple endpoints as fallback
 */
export const getTeamLeaders = async (year: number, seasonType: number, teamId: string) => {
  // Try the v2 core API endpoint first
  try {
    const url = `https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/${year}/types/${seasonType}/teams/${teamId}/leaders`;
    const response = await fetch(url);
    
    if (response.ok) {
      const data = await response.json();
      // Check if we got valid data - accept any object with meaningful structure
      if (data && typeof data === 'object' && (data.items || data.categories || data.leaders || data.$ref || Object.keys(data).length > 0)) {
        return data;
      }
    } else {
      console.warn(`Response not OK: ${response.status} ${response.statusText}`);
    }
  } catch (error) {
    console.warn(`Primary team leaders endpoint failed for ${teamId}:`, error);
  }

  // Fallback: Try the site API v3 endpoint
  try {
    const url = `https://site.web.api.espn.com/apis/site/v3/sports/football/nfl/teamleaders?teamId=${teamId}`;
    const response = await fetch(url);
    
    if (response.ok) {
      const data = await response.json();
      if (data && (data.items || data.categories || data.leaders || data.data)) {
        return data;
      }
    }
  } catch (error) {
    console.warn(`Fallback team leaders endpoint failed for ${teamId}:`, error);
  }

  // Return null instead of throwing - we'll handle empty data gracefully
  console.warn(`All team leaders endpoints failed for team ${teamId}, returning null`);
  return null;
};

/**
 * Fetch team events (games) from ESPN API
 */
export const getTeamEvents = async (year: number, teamId: string) => {
  try {
    const url = `https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/${year}/teams/${teamId}/events`;
    const response = await fetch(url);
    
    if (!response.ok) {
      console.warn(`Team events response not OK: ${response.status} ${response.statusText}`);
      return null;
    }
    const data = await response.json();
    return data;
  } catch (error) {
    console.error(`Error fetching team events for ${teamId}:`, error);
    return null; // Return null instead of throwing
  }
};

/**
 * Fetch injury report for a team from ESPN API
 * Tries multiple endpoints as fallback
 */
export const getTeamInjuries = async (teamId: string) => {
  // Try the site API v2 endpoint first
  try {
    const url = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/${teamId}/injuries`;
    const response = await fetch(url);
    
    if (response.ok) {
      const data = await response.json();
      // Check if we got valid data - support entries, injuries, items, or direct array
      if (data && (data.entries || data.injuries || data.items || Array.isArray(data) || (typeof data === 'object' && Object.keys(data).length > 0))) {
        return data;
      }
    } else {
      console.warn(`Response not OK: ${response.status} ${response.statusText}`);
    }
  } catch (error) {
    console.warn(`Primary injuries endpoint failed for ${teamId}:`, error);
  }

  // Fallback: Try the core API endpoint
  try {
    const url = `https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/${teamId}/injuries`;
    const response = await fetch(url);
    
    if (response.ok) {
      const data = await response.json();
      // Check if we got valid data - support entries, injuries, items, or direct array
      if (data && (data.entries || data.injuries || data.items || Array.isArray(data))) {
        return data;
      }
    }
  } catch (error) {
    console.warn(`Fallback injuries endpoint failed for ${teamId}:`, error);
  }

  // Return empty structure if all endpoints fail
  console.warn(`All injury endpoints failed for team ${teamId}, returning empty`);
  return { entries: [] };
};

/**
 * Fetch boxscore for a game from ESPN API
 */
export const getGameBoxscore = async (eventId: string) => {
  try {
    const url = `https://cdn.espn.com/core/nfl/boxscore?xhr=1&gameId=${eventId}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch boxscore: ${response.statusText}`);
    }
    return response.json();
  } catch (error) {
    console.error(`Error fetching boxscore for ${eventId}:`, error);
    throw error;
  }
};

/**
 * Fetch game leaders (stats for a specific game) from ESPN API
 * This is for completed games only
 */
/**
 * Fetch team statistics for a completed game from boxscore
 * Uses the summary endpoint which includes boxscore data
 */
export const getTeamStatistics = async (eventId: string) => {
  try {
    const url = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=${eventId}`;
    const response = await fetch(url);
    
    if (!response.ok) {
      console.warn(`[getTeamStatistics] Failed to fetch statistics: ${response.status}`);
      return null;
    }
    
    const data = await response.json();
    const boxscore = data?.boxscore;
    
    if (!boxscore || !boxscore.teams || boxscore.teams.length < 2) {
      console.warn(`[getTeamStatistics] No team statistics found in boxscore`);
      return null;
    }
    
    // Extract statistics for both teams
    const teams = boxscore.teams;
    const homeTeam = teams.find((t: any) => t.team?.homeAway === "home") || teams[0];
    const awayTeam = teams.find((t: any) => t.team?.homeAway === "away") || teams[1];
    
    const getStat = (team: any, statName: string) => {
      const stats = team.statistics || [];
      return stats.find((s: any) => s.name === statName);
    };
    
    const getStatValue = (team: any, statName: string): number | null => {
      const stat = getStat(team, statName);
      if (!stat) return null;
      
      // Try to get numeric value from value field
      if (stat.value != null && stat.value !== '-' && stat.value !== '') {
        const numValue = Number(stat.value);
        if (!isNaN(numValue)) {
          return numValue;
        }
      }
      
      // Fallback: try to parse displayValue if value is not a valid number
      if (stat.displayValue) {
        // Remove any non-numeric characters except minus sign at the start
        const cleaned = stat.displayValue.replace(/[^\d.-]/g, '');
        const numValue = Number(cleaned);
        if (!isNaN(numValue)) {
          return numValue;
        }
      }
      
      return null;
    };
    
    const getStatDisplay = (team: any, statName: string): string | null => {
      const stat = getStat(team, statName);
      return stat?.displayValue || null;
    };
    
    // Extract red zone attempts and successes
    const homeRedZone = getStatDisplay(homeTeam, "redZoneAttempts");
    const awayRedZone = getStatDisplay(awayTeam, "redZoneAttempts");
    
    // Format possession time (convert from seconds to MM:SS)
    const formatPossession = (seconds: number | null): string => {
      if (seconds == null) return "—";
      const mins = Math.floor(seconds / 60);
      const secs = Math.floor(seconds % 60);
      return `${mins}:${secs.toString().padStart(2, "0")}`;
    };
    
    const homePossession = getStatValue(homeTeam, "possessionTime");
    const awayPossession = getStatValue(awayTeam, "possessionTime");
    
    return {
      homeTeam: {
        name: homeTeam.team?.displayName || homeTeam.team?.name,
        abbreviation: homeTeam.team?.abbreviation,
        logo: homeTeam.team?.logo,
        totalYards: getStatValue(homeTeam, "totalYards"),
        turnovers: getStatValue(homeTeam, "turnovers"),
        firstDowns: getStatValue(homeTeam, "firstDowns"),
        penalties: getStatDisplay(homeTeam, "totalPenaltiesYards"),
        thirdDown: getStatDisplay(homeTeam, "thirdDownEff"),
        fourthDown: getStatDisplay(homeTeam, "fourthDownEff"),
        redZone: homeRedZone,
        possession: formatPossession(homePossession),
      },
      awayTeam: {
        name: awayTeam.team?.displayName || awayTeam.team?.name,
        abbreviation: awayTeam.team?.abbreviation,
        logo: awayTeam.team?.logo,
        totalYards: getStatValue(awayTeam, "totalYards"),
        turnovers: getStatValue(awayTeam, "turnovers"),
        firstDowns: getStatValue(awayTeam, "firstDowns"),
        penalties: getStatDisplay(awayTeam, "totalPenaltiesYards"),
        thirdDown: getStatDisplay(awayTeam, "thirdDownEff"),
        fourthDown: getStatDisplay(awayTeam, "fourthDownEff"),
        redZone: awayRedZone,
        possession: formatPossession(awayPossession),
      },
    };
  } catch (error) {
    console.error(`[getTeamStatistics] Error fetching team statistics for event ${eventId}:`, error);
    return null;
  }
};

export const getGameLeaders = async (eventId: string, teamId: string) => {
  try {
    // ------------------------------------------------------------------
    // Primary source: competition details & statistics (per-game)
    // We intentionally skip the /events/{id}/leaders endpoint to avoid
    // noisy 404s in the browser network console. All game-leader data
    // is derived from per-game stats and boxscore.
    // ------------------------------------------------------------------
    // First, get the competition details
    const gameDetails = await getGameDetails(eventId);
    
    const competition = gameDetails.competitions?.[0];
    
    if (!competition) {
      console.warn(`[getGameLeaders] No competition found in game details`);
      return null;
    }


    // If competition has $ref, fetch it
    let competitionData = competition;
    if (competition.$ref && Object.keys(competition).length === 1) {
      const compResponse = await fetch(ensureHttps(competition.$ref));
      if (compResponse.ok) {
        competitionData = await compResponse.json();
      }
    }

    // Get competitors and find the team
    const competitors = competitionData.competitors || [];
    
    const teamCompetitor = competitors.find((c: any) => {
      const cTeamId = c.team?.id || c.team?.$ref?.match(/\/teams\/(\d+)/)?.[1];
      const matches = cTeamId === teamId;
      if (matches) {
      }
      return matches;
    });

    if (!teamCompetitor) {
      console.warn(`[getGameLeaders] Team competitor not found for teamId: ${teamId}`);
      return null;
    }


    // Try to get statistics from competitor
    let statsData = null;
    if (teamCompetitor.statistics?.$ref) {
      try {
        const statsResponse = await fetch(ensureHttps(teamCompetitor.statistics.$ref));
        if (statsResponse.ok) {
          statsData = await statsResponse.json();
        } else {
          console.warn(`[getGameLeaders] Statistics response not OK:`, statsResponse.status);
        }
      } catch (error) {
        console.warn(`[getGameLeaders] Failed to fetch statistics from $ref:`, error);
      }
    } else if (teamCompetitor.statistics) {
      statsData = teamCompetitor.statistics;
    } else {
      console.warn(`[getGameLeaders] No statistics found on competitor`);
    }

    // If we have stats, try to get leaders
      if (statsData) {
      
      // Try to get leaders from stats
      if (statsData.leaders?.$ref) {
        try {
          const leadersResponse = await fetch(ensureHttps(statsData.leaders.$ref));
          if (leadersResponse.ok) {
            const leadersData = await leadersResponse.json();
            return leadersData;
          } else {
            console.warn(`[getGameLeaders] Leaders response not OK:`, leadersResponse.status);
          }
        } catch (error) {
          console.warn(`[getGameLeaders] Failed to fetch leaders from $ref:`, error);
        }
      } else if (statsData.leaders) {
        return statsData.leaders;
      }
      
      // Try to extract leaders from splits data
      if (statsData.splits) {
        let splitsData = statsData.splits;
        
        // If splits is a $ref, fetch it
        if (splitsData.$ref && Object.keys(splitsData).length === 1) {
          try {
            const splitsResponse = await fetch(ensureHttps(splitsData.$ref));
            if (splitsResponse.ok) {
              splitsData = await splitsResponse.json();
            }
          } catch (error) {
            console.warn(`[getGameLeaders] Failed to fetch splits:`, error);
          }
        }
        
        // Extract categories from splits
        const splitsCategories = splitsData.categories || splitsData.items || (Array.isArray(splitsData) ? splitsData : []);
        
        if (splitsCategories.length > 0) {
          const leaderItems: any[] = [];
          
          splitsCategories.forEach((category: any, index: number) => {
            const categoryName = (category.name || category.displayName || category.abbreviation || category.statType || '').toLowerCase();
            
            // Get the top player from this category
            const athletes = category.athletes || category.leaders || category.stats || [];
            if (Array.isArray(athletes) && athletes.length > 0) {
              // Sort by value/stat and get the top one
              const sortedAthletes = [...athletes].sort((a: any, b: any) => {
                const aValue = a.value || a.statValue || a.statistics?.value || a.stat || 0;
                const bValue = b.value || b.statValue || b.statistics?.value || b.stat || 0;
                return bValue - aValue;
              });
              
              const topAthlete = sortedAthletes[0];
              if (topAthlete) {
                leaderItems.push({
                  displayName: category.displayName || category.name || categoryName,
                  leaders: [topAthlete],
                });
              }
            }
          });
          
          if (leaderItems.length > 0) {
            return { items: leaderItems };
          } else {
            console.warn(`[getGameLeaders] No athletes found in splits categories`);
          }
        } else {
          console.warn(`[getGameLeaders] No categories found in splits data`);
        }
      } else {
        console.warn(`[getGameLeaders] No leaders or splits found in stats data`);
      }
    }

    // Fallback: Try the boxscore endpoint
    try {
      const boxscore = await getGameBoxscore(eventId);
      
      // Extract team stats from boxscore if available
      if (boxscore?.gamepackageJSON?.boxscore) {
        const boxscoreData = boxscore.gamepackageJSON.boxscore;
        const teams = boxscoreData.teams || [];
        const teamStats = teams.find((t: any) => {
          const tId = t.team?.id || String(t.team?.id);
          return tId === teamId || String(tId) === String(teamId);
        });
        
        if (teamStats) {
          if (teamStats.leaders) {
            return teamStats.leaders;
          }
        } else {
          console.warn(`[getGameLeaders] Team stats not found in boxscore for teamId: ${teamId}`);
        }
      } else {
        console.warn(`[getGameLeaders] No boxscore data in response`);
      }

      // Additional fallback: use gamepackageJSON.leaders (includes both teams)
      if (boxscore?.gamepackageJSON?.leaders) {
        const overallLeaders = boxscore.gamepackageJSON.leaders;
        const leaderCategories = Array.isArray(overallLeaders) ? overallLeaders : (overallLeaders.categories || overallLeaders.items || []);

        const teamLeaderItems: any[] = [];
        leaderCategories.forEach((category: any, index: number) => {
          if (!category?.leaders || category.leaders.length === 0) {
            return;
          }

          const matchingLeader = category.leaders.find((leader: any) => {
            const leaderTeamId =
              leader.team?.id ||
              leader.teamId ||
              leader.team?.$ref?.match(/\/teams\/(\d+)/)?.[1];
            const matches = String(leaderTeamId) === String(teamId);
            if (matches) {
            }
            return matches;
          });

          if (matchingLeader) {
            teamLeaderItems.push({
              displayName:
                category.displayName ||
                category.name ||
                category.shortDisplayName ||
                category.abbreviation ||
                "Leader",
              leaders: [matchingLeader],
            });
          }
        });

        if (teamLeaderItems.length > 0) {
          return { items: teamLeaderItems };
        } else {
          console.warn(
            `[getGameLeaders] No matching leaders found in overall leaders for teamId: ${teamId}. Available team IDs in leaders:`,
            leaderCategories.flatMap((cat: any) => 
              cat.leaders?.map((l: any) => l.team?.id || l.teamId || 'unknown') || []
            )
          );
        }
      }
    } catch (error) {
      console.warn(`[getGameLeaders] Failed to fetch boxscore for game leaders:`, error);
    }

    console.warn(`[getGameLeaders] All methods failed, returning null`);
    return null;
  } catch (error) {
    console.error(`[getGameLeaders] Error fetching game leaders for event ${eventId}, team ${teamId}:`, error);
    return null;
  }
};

