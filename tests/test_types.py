# type: ignore
"""Tests for aredis_om.model.types – Coordinates and GeoFilter."""

import pytest
from pydantic import TypeAdapter

from aredis_om.model.types import Coordinates, GeoFilter

# ---------------------------------------------------------------------------
# Coordinates
# ---------------------------------------------------------------------------


class TestCoordinates:
    def test_create_valid_coordinates(self):
        c = Coordinates(latitude=45.5, longitude=-122.6)
        assert c.latitude == 45.5
        assert c.longitude == -122.6

    def test_str_format_lon_lat(self):
        c = Coordinates(latitude=45.5, longitude=-122.6)
        assert str(c) == "-122.6,45.5"

    def test_invalid_latitude_raises(self):
        with pytest.raises(ValueError, match="Latitude"):
            Coordinates(latitude=90.0, longitude=0.0)

    def test_invalid_longitude_raises(self):
        with pytest.raises(ValueError, match="Longitude"):
            Coordinates(latitude=0.0, longitude=180.0)

    def test_validate_from_coordinates_instance(self):
        original = Coordinates(latitude=10.0, longitude=20.0)
        result = Coordinates.validate(original)
        assert result is original

    def test_validate_from_string(self):
        result = Coordinates.validate("-122.6,45.5")
        assert result.longitude == -122.6
        assert result.latitude == 45.5

    def test_validate_from_bad_string_raises(self):
        with pytest.raises(ValueError, match="Invalid coordinate format"):
            Coordinates.validate("bad")

    def test_validate_from_non_numeric_string_raises(self):
        with pytest.raises(ValueError, match="Invalid coordinate values"):
            Coordinates.validate("abc,def")

    def test_validate_from_tuple(self):
        result = Coordinates.validate((45.5, -122.6))
        assert result.latitude == 45.5
        assert result.longitude == -122.6

    def test_validate_from_dict_lat_lon(self):
        result = Coordinates.validate({"latitude": 45.5, "longitude": -122.6})
        assert result.latitude == 45.5
        assert result.longitude == -122.6

    def test_validate_from_dict_short_keys(self):
        result = Coordinates.validate({"lat": 45.5, "lon": -122.6})
        assert result.latitude == 45.5
        assert result.longitude == -122.6

    def test_validate_from_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="Coordinates must be provided"):
            Coordinates.validate(12345)

    def test_type_adapter_uses_pydantic_v2_schema(self):
        result = TypeAdapter(Coordinates).validate_python("-122.6,45.5")
        assert result == Coordinates(latitude=45.5, longitude=-122.6)


# ---------------------------------------------------------------------------
# GeoFilter
# ---------------------------------------------------------------------------


class TestGeoFilter:
    def test_create_valid_geofilter(self):
        gf = GeoFilter(longitude=-122.6, latitude=45.5, radius=10.0, unit="km")
        assert gf.longitude == -122.6
        assert gf.latitude == 45.5
        assert gf.radius == 10.0
        assert gf.unit == "km"

    def test_str_format(self):
        gf = GeoFilter(longitude=-122.6, latitude=45.5, radius=10.0, unit="km")
        assert str(gf) == "-122.6 45.5 10.0 km"

    def test_invalid_longitude_raises(self):
        with pytest.raises(ValueError, match="Longitude"):
            GeoFilter(longitude=200, latitude=0, radius=10, unit="km")

    def test_invalid_latitude_raises(self):
        with pytest.raises(ValueError, match="Latitude"):
            GeoFilter(longitude=0, latitude=100, radius=10, unit="km")

    def test_invalid_radius_raises(self):
        with pytest.raises(ValueError, match="Radius"):
            GeoFilter(longitude=0, latitude=0, radius=-1, unit="km")

    def test_from_coordinates(self):
        coords = Coordinates(latitude=45.5, longitude=-122.6)
        gf = GeoFilter.from_coordinates(coords, radius=5.0, unit="mi")
        assert gf.longitude == -122.6
        assert gf.latitude == 45.5
        assert gf.radius == 5.0
        assert gf.unit == "mi"

    def test_all_units_accepted(self):
        for unit in ("m", "km", "mi", "ft"):
            gf = GeoFilter(longitude=0, latitude=0, radius=1, unit=unit)
            assert gf.unit == unit
