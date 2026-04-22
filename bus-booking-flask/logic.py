from dataclasses import dataclass
import difflib
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class LogicService:
    """
    Keep notebook logic in this service.
    Replace TODO sections with your existing prototype logic.
    """

    dataset_path: Path
    df: pd.DataFrame
    important_stops: list[str]
    all_stops: list[str]
    nearest_map: dict[str, list[str]]
    major_hubs: list[str]

    @classmethod
    def from_excel(cls, dataset_path: str) -> "LogicService":
        path = Path(dataset_path)
        # Dataset loads once at startup.
        df = pd.read_excel(path)
        return cls(
            dataset_path=path,
            df=df,
            important_stops=[],
            all_stops=[],
            nearest_map={},
            major_hubs=["majestic", "silkboard", "marathahalli", "whitefield"],
        )

    @staticmethod
    def split_stops(stops: Any) -> list[str]:
        if not isinstance(stops, str):
            return []
        if "," in stops:
            return [s.strip().lower() for s in stops.split(",") if s.strip()]
        return [s.strip().lower() for s in stops.split() if s.strip()]

    def preprocess_stops(self) -> None:
        if "Stops" in self.df.columns:
            self.df["Stops"] = self.df["Stops"].astype(str).str.lower()

        self.important_stops = [
            "attavar",
            "atavar",
            "pandeshwar",
            "jyothi",
            "lalbagh",
            "kottara",
            "surathkal",
            "pumpwell",
            "statebank",
        ]

        self.nearest_map = {
            "pandeshwar": ["jyothi", "lalbagh"],
            "kankanady": ["lalbagh", "jyothi"],
            "pumpwell": ["statebank"],
            "btm": ["silk board", "silkboard"],
            "indiranagar": ["marathahalli", "majestic"],
            "attavar": ["jyothi", "lalbagh"],
            "atavar": ["jyothi", "lalbagh"],
            "falnir": ["jyothi"],
            "hampankatta": ["jyothi"],
        }

        all_stops: set[str] = set()
        for stops in self.df.get("Stops", []):
            for stop in self.split_stops(stops):
                all_stops.add(stop)
        self.all_stops = sorted(all_stops)

    def fuzzy_correct_stop(self, user_text: str) -> str:
        cleaned = user_text.strip().lower()
        if not cleaned:
            return cleaned

        match = difflib.get_close_matches(
            cleaned, self.important_stops, n=1, cutoff=0.5
        )
        if match:
            return match[0]

        match = difflib.get_close_matches(cleaned, self.all_stops, n=1, cutoff=0.6)
        return match[0] if match else cleaned

    def _get_nearest_stop(self, user_input: str, stops: Any) -> str | None:
        stops_list = self.split_stops(stops)
        if not stops_list:
            return None

        if user_input in stops_list:
            return user_input

        if user_input in self.nearest_map:
            for mapped in self.nearest_map[user_input]:
                mapped_variants = [mapped, mapped.replace(" ", "")]
                for candidate in mapped_variants:
                    if candidate in stops_list:
                        return candidate

        match = difflib.get_close_matches(user_input, stops_list, n=1, cutoff=0.5)
        return match[0] if match else None

    def _get_best_drop(self, destination: str, stops: Any) -> str | None:
        stops_list = self.split_stops(stops)
        if not stops_list:
            return None

        if destination in stops_list:
            return destination

        for hub in self.major_hubs:
            if hub in stops_list:
                return hub

        match = difflib.get_close_matches(destination, stops_list, n=1, cutoff=0.3)
        return match[0] if match else None

    def find_nearest_stops(self, source: str, destination: str) -> dict[str, Any]:
        best_pickup = None
        best_drop = None
        is_valid_order = False

        for _, row in self.df.iterrows():
            stops_list = self.split_stops(row.get("Stops"))
            pickup = self._get_nearest_stop(source, row.get("Stops"))
            drop = self._get_best_drop(destination, row.get("Stops"))
            if not pickup or not drop:
                continue

            if pickup in stops_list and drop in stops_list:
                if stops_list.index(pickup) < stops_list.index(drop):
                    best_pickup = pickup
                    best_drop = drop
                    is_valid_order = True
                    break

        return {
            "pickup_stop": best_pickup or source,
            "drop_stop": best_drop or destination,
            "is_valid_order": is_valid_order,
        }

    def fallback_via_major_stop(self, source: str, destination: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        via_hub = "majestic"

        for _, row in self.df.iterrows():
            stops_list = self.split_stops(row.get("Stops"))
            pickup = self._get_nearest_stop(source, row.get("Stops"))
            if not pickup or via_hub not in stops_list:
                continue
            if pickup not in stops_list:
                continue
            if stops_list.index(pickup) >= stops_list.index(via_hub):
                continue

            duration = float(row.get("Duration_hrs", 0) or 0)
            results.append(
                {
                    "route_name": str(row.get("Bus_No", "Unknown")),
                    "bus_no": str(row.get("Bus_No", "Unknown")),
                    "operator": str(row.get("Operator", "Unknown")),
                    "pickup_stop": pickup,
                    "drop_stop": via_hub,
                    "departure": str(row.get("Departure", "")),
                    "duration_hrs": duration,
                    "distance_km": round(duration * 42, 1),
                    "estimated_fare": int(max(50, round(duration * 35))),
                    "via_fallback": True,
                }
            )

        return sorted(results, key=lambda x: (x["duration_hrs"], x["estimated_fare"]))

    def recommend_parcel(
        self,
        source: str,
        destination: str,
        parcel_type: str = "",
        limit: int = 3,
    ) -> list[dict[str, Any]]:
        """
        Main function adapted from notebook:
        - accepts parameters (no input())
        - returns structured results (no print())
        """
        source_clean = self.fuzzy_correct_stop(source)
        destination_clean = self.fuzzy_correct_stop(destination)

        direct_results: list[dict[str, Any]] = []
        nearest_results: list[dict[str, Any]] = []

        for _, row in self.df.iterrows():
            stops_list = self.split_stops(row.get("Stops"))
            pickup = self._get_nearest_stop(source_clean, row.get("Stops"))
            if not pickup:
                continue

            # Keep pickup around route start for better relevance.
            if pickup not in stops_list[:4]:
                continue

            drop = self._get_best_drop(destination_clean, row.get("Stops"))
            if not drop:
                continue

            if pickup not in stops_list or drop not in stops_list:
                continue
            if stops_list.index(pickup) >= stops_list.index(drop):
                continue

            duration = float(row.get("Duration_hrs", 0) or 0)
            item = {
                "route_name": str(row.get("Bus_No", "Unknown")),
                "bus_no": str(row.get("Bus_No", "Unknown")),
                "operator": str(row.get("Operator", "Unknown")),
                "pickup_stop": pickup,
                "drop_stop": drop,
                "departure": str(row.get("Departure", "")),
                "duration_hrs": duration,
                "distance_km": round(duration * 42, 1),
                "estimated_fare": int(max(50, round(duration * 35))),
                "is_direct_drop": destination_clean in stops_list,
            }

            if item["is_direct_drop"]:
                direct_results.append(item)
            else:
                nearest_results.append(item)

        if direct_results:
            ranked = sorted(
                direct_results, key=lambda x: (x["duration_hrs"], x["estimated_fare"])
            )
            return ranked[:limit]

        if nearest_results:
            ranked = sorted(
                nearest_results, key=lambda x: (x["duration_hrs"], x["estimated_fare"])
            )
            return ranked[:limit]

        fallback = self.fallback_via_major_stop(source_clean, destination_clean)
        return fallback[:limit]

    def chat_show_results(
        self, source: str, destination: str, parcel_type: str = ""
    ) -> dict[str, Any]:
        """
        Optional adapter if your notebook uses chat_show_results().
        Returns dictionary payload for Flask templates/API usage.
        """
        recommendations = self.recommend_parcel(source, destination, parcel_type, limit=3)
        return {
            "query": {
                "source": source,
                "destination": destination,
                "parcel_type": parcel_type,
            },
            "recommendations": recommendations,
        }


_logic_service: LogicService | None = None


def get_logic_service(dataset_path: str) -> LogicService:
    global _logic_service
    if _logic_service is None:
        _logic_service = LogicService.from_excel(dataset_path)
        _logic_service.preprocess_stops()
    return _logic_service
