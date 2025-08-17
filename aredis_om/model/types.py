from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Tuple, Union


RadiusUnit = Literal["m", "km", "mi", "ft"]


@dataclass
class Coordinates:
    """
    Coordinates dataclass: Latitude and Longitude
    NOTE: Do not change lat, long range values. They are standard
    """

    latitude: float
    longitude: float

    @classmethod
    def __get_validators__(cls):  # type: ignore[override]
        yield cls.validate

    @classmethod
    def validate(cls, v: Any) -> "Coordinates":
        if isinstance(v, Coordinates):
            return v
        if isinstance(v, str):
            parts = v.split(",")
            if len(parts) != 2:
                raise ValueError(
                    f"Invalid coordinate format. Expected 'longitude,latitude' but got: {v}"
                )
            lon_s, lat_s = parts[0], parts[1]
            try:
                lon = float(lon_s)
                lat = float(lat_s)
            except ValueError:
                raise ValueError(
                    f"Invalid coordinate values. Expected floats but got: {v}"
                )
            return cls(latitude=lat, longitude=lon)
        if isinstance(v, tuple) and len(v) == 2:
            lat, lon = v  # assume conventional (lat, lon)
            return cls(latitude=float(lat), longitude=float(lon))
        if isinstance(v, dict):
            if "latitude" in v and "longitude" in v:
                return cls(latitude=float(v["latitude"]), longitude=float(v["longitude"]))
            if "lat" in v and "lon" in v:
                return cls(latitude=float(v["lat"]), longitude=float(v["lon"]))
        raise TypeError(
            "Coordinates must be provided as 'lon,lat' string, (lat, lon) tuple, or dict with latitude/longitude"
        )

    def __post_init__(self):
        if not (-85.05112878 < self.latitude < 85.05112878):
            raise ValueError(
                f"Latitude must be between -85.05112878 and 85.05112878, got {self.latitude}"
            )
        if not (-180 < self.longitude < 180):
            raise ValueError(
                f"Longitude must be between -180 and 180, got {self.longitude}"
            )

    def __str__(self) -> str:
        # Redis expects "lon,lat" for storage and "lon lat radius unit" in queries
        return f"{self.longitude},{self.latitude}"


class GeoFilter:
    """
    A geographic filter for searching within a radius of a coordinate point.
    Used with GEO fields to find models within a specified distance from a location.

    Example:
        # Find all locations within 10 miles of Portland, OR
        # results = await Location.find(
        #     Location.coordinates == GeoFilter(longitude=-122.6765, latitude=45.5231, radius=10, unit="mi")
        # ).all()
    """

    def __init__(self, longitude: float, latitude: float, radius: float, unit: RadiusUnit):
        if not -180 <= longitude <= 180:
            raise ValueError(f"Longitude must be between -180 and 180, got {longitude}")
        if not -90 <= latitude <= 90:
            raise ValueError(f"Latitude must be between -90 and 90, got {latitude}")
        if radius <= 0:
            raise ValueError(f"Radius must be positive, got {radius}")

        self.longitude = float(longitude)
        self.latitude = float(latitude)
        self.radius = float(radius)
        self.unit = unit

    def __str__(self) -> str:
        # RediSearch GEO filter syntax: "lon lat radius unit"
        return f"{self.longitude} {self.latitude} {self.radius} {self.unit}"

    @classmethod
    def from_coordinates(cls, coords: Coordinates, radius: float, unit: RadiusUnit) -> "GeoFilter":
        return cls(coords.longitude, coords.latitude, radius, unit)

