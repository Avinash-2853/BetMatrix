import { useQuery } from "@tanstack/react-query";
import { AvailableDataResponse, getAvailableData, getPredictions, PredictionsListResponse } from "@/lib/api";

const isValidSelection = (value: number | null | undefined): value is number =>
  typeof value === "number" && !Number.isNaN(value);

export const useAvailableData = () =>
  useQuery<AvailableDataResponse>({
    queryKey: ["available-data"],
    queryFn: getAvailableData,
  });

export const usePredictions = (year?: number | null, week?: number | null) =>
  useQuery<PredictionsListResponse>({
    queryKey: ["predictions", year, week],
    queryFn: () => getPredictions(year as number, week as number),
    enabled: isValidSelection(year) && isValidSelection(week),
    staleTime: 5 * 60 * 1000,
  });

