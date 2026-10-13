r"""Build data/qgis/gbdemo.qgz: the pipeline's inputs, checks and outputs, styled, in QGIS.

Run qgis/export_layers.py first, then with QGIS's Python:
    "C:\Program Files\QGIS 3.44.13\bin\python-qgis-ltr.bat" qgis\build_project.py [--render]

--render also writes data/qgis/preview.png (the project's opening view) to check the styling.

Groups, top to bottom (relative paths, so the project moves with data/qgis/):
  Outputs (gold)    disclosure-controlled results: real and synthetic registered per resident by LSOA,
                    new postcodes since the Census, districts' monthly change, practices, MSOA, ICB
  Checks (silver)   spatial QA on public postcodes: no LSOA match (in water) and the 2025 disagreements
                    that ONS corrected in 2026
  Inputs            postcodes (live / terminated), Census residents, ODS organisations (GP practices vs
                    the minor injury units, prison and pharmacies the generator bug sent patients to),
                    LSOA / MSOA / ICB boundaries
  Basemap           OpenStreetMap
"""

import sys
from pathlib import Path

from qgis.core import (
    QgsApplication,
    QgsCategorizedSymbolRenderer,
    QgsCoordinateReferenceSystem,
    QgsFillSymbol,
    QgsGraduatedSymbolRenderer,
    QgsMapRendererParallelJob,
    QgsMapSettings,
    QgsMarkerSymbol,
    QgsPalLayerSettings,
    QgsProject,
    QgsRasterLayer,
    QgsRectangle,
    QgsRendererCategory,
    QgsRendererRange,
    QgsSingleSymbolRenderer,
    QgsTextFormat,
    QgsVectorLayer,
    QgsVectorLayerSimpleLabeling,
)
from qgis.PyQt.QtCore import QSize
from qgis.PyQt.QtGui import QColor

HERE = Path(__file__).resolve().parents[1] / "data" / "qgis"
GPKG = HERE / "gbdemo.gpkg"
OUT = HERE / "gbdemo.qgz"
RATIO_BREAKS = [  # same scale for synthetic and real, as on the results site
    (0.0, 0.95, "#f0a35e", "under 0.95 (fewer registered than residents)"),
    (0.95, 1.00, "#e8f0fb", "0.95 - 1.00"),
    (1.00, 1.05, "#b7d3f6", "1.00 - 1.05"),
    (1.05, 1.10, "#6da7ec", "1.05 - 1.10"),
    (1.10, 1.25, "#2a78d6", "1.10 - 1.25"),
    (1.25, 99.0, "#184f95", "1.25 and over"),
]


def layer(name: str, title: str) -> QgsVectorLayer:
    lyr = QgsVectorLayer(f"{GPKG}|layername={name}", title, "ogr")
    if not lyr.isValid():
        raise RuntimeError(f"layer {name} not found in {GPKG}: run qgis/export_layers.py first")
    return lyr


def fill(color: str, outline: str = "#ffffff", width: float = 0.15, alpha: int = 255) -> QgsFillSymbol:
    s = QgsFillSymbol.createSimple({"color": color, "outline_color": outline, "outline_width": str(width)})
    s.setOpacity(alpha / 255)
    return s


def outline(color: str, width: float = 0.3, style: str = "solid") -> QgsFillSymbol:
    return QgsFillSymbol.createSimple(
        {"color": "0,0,0,0", "outline_color": color, "outline_width": str(width), "outline_style": style}
    )


def marker(color: str, size: float, shape: str = "circle", stroke: str = "#ffffff") -> QgsMarkerSymbol:
    return QgsMarkerSymbol.createSimple(
        {"name": shape, "color": color, "size": str(size), "outline_color": stroke}
    )


def graduated(lyr, field: str, breaks, symbol=fill, opacity: float = 0.8) -> QgsVectorLayer:
    ranges = [QgsRendererRange(lo, hi, symbol(color), label) for lo, hi, color, label in breaks]
    lyr.setRenderer(QgsGraduatedSymbolRenderer(field, ranges))
    lyr.setOpacity(opacity)
    return lyr


def categorized(lyr, field: str, cats) -> QgsVectorLayer:
    lyr.setRenderer(
        QgsCategorizedSymbolRenderer(field, [QgsRendererCategory(v, s, label) for v, s, label in cats])
    )
    return lyr


def labels(
    lyr, expression: str, size: float = 8, color: str = "#1d2731", max_scale: float = 0
) -> QgsVectorLayer:
    s = QgsPalLayerSettings()
    s.fieldName, s.isExpression = expression, True
    fmt = QgsTextFormat()
    fmt.setSize(size)
    fmt.setColor(QColor(color))
    buf = fmt.buffer()
    buf.setEnabled(True)
    buf.setSize(0.8)
    s.setFormat(fmt)
    if max_scale:
        s.scaleVisibility, s.minimumScale, s.maximumScale = True, max_scale, 0
    lyr.setLabeling(QgsVectorLayerSimpleLabeling(s))
    lyr.setLabelsEnabled(True)
    return lyr


def scale_limit(lyr, max_scale: float) -> QgsVectorLayer:
    """Draw only when zoomed in beyond 1:max_scale (dense point layers)."""
    lyr.setScaleBasedVisibility(True)
    lyr.setMinimumScale(max_scale)
    return lyr


def build(project: QgsProject) -> QgsRectangle:
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:27700"))
    project.setTitle("GB demographic & geospatial pipeline: inputs, checks and outputs")
    root = project.layerTreeRoot()

    def add(group_name: str, layers: list, visible_ids: set) -> None:
        group = root.addGroup(group_name)
        for lyr in layers:
            project.addMapLayer(lyr, False)
            node = group.addLayer(lyr)
            node.setItemVisibilityChecked(lyr.name() in visible_ids)
            node.setExpanded(False)

    # ---------------- outputs (gold)
    real = graduated(
        layer("out_lsoa", "Real registered per resident by LSOA (NHS England)"),
        "nhs_per_resident",
        RATIO_BREAKS,
    )
    synth = graduated(
        layer("out_lsoa", "Synthetic registered per resident by LSOA"),
        "registered_per_resident",
        RATIO_BREAKS,
    )
    newpc = graduated(
        layer("out_lsoa", "Postcodes introduced since Census day, by LSOA"),
        "postcodes_since_census",
        [
            (0, 0.5, "#f2f2f2", "none"),
            (0.5, 5, "#d9c6ec", "1 - 4"),
            (5, 10, "#a98bd6", "5 - 9"),
            (10, 25, "#7650b8", "10 - 24"),
            (25, 1000, "#46237a", "25 and over"),
        ],
    )
    # districts: outline + label only, so the LSOA colours underneath stay readable
    districts = labels(
        layer("out_districts_monthly", "Districts: change in registered patients, Jul to Sep 2026"),
        "district || '\\n+' || format_number(change_over_period, 0) || ' patients'",
        size=10,
    )
    districts.setRenderer(QgsSingleSymbolRenderer(outline("#222222", 0.9)))
    practices = labels(
        graduated(
            layer("out_practices", "GP practices: synthetic minus real patients from the study area"),
            "synthetic_minus_real",
            [
                (-99999, -5000, "#b2182b", "synthetic far below real (under -5,000)"),
                (-5000, -1000, "#ef8a62", "-5,000 to -1,000"),
                (-1000, 1000, "#f7f7f7", "within 1,000"),
                (1000, 5000, "#67a9cf", "1,000 to 5,000"),
                (5000, 99999, "#2166ac", "synthetic far above real (over 5,000)"),
            ],
            symbol=lambda c: marker(c, 3.2, stroke="#333333"),
            opacity=1.0,
        ),
        "practice_name",
        size=7,
        max_scale=60000,
    )
    msoa = graduated(
        layer("out_msoa", "Synthetic registered per resident by MSOA"),
        "registered_per_resident",
        RATIO_BREAKS,
    )
    icb = labels(
        graduated(
            layer("out_icb", "ICB: synthetic registered per resident"),
            "registered_per_resident",
            RATIO_BREAKS,
        ),
        "icb_name",
    )
    add(
        "Outputs (gold: disclosure-controlled)",
        [districts, practices, real, synth, newpc, msoa, icb],
        {real.name(), districts.name()},
    )

    # ---------------- checks (silver)
    nomatch = labels(layer("check_no_match", "Check: postcodes in no LSOA (tidal water)"), "postcode", size=7)
    nomatch.setRenderer(
        QgsCategorizedSymbolRenderer(
            "lsoa_resolution",
            [
                QgsRendererCategory(
                    "no_match", marker("#d7191c", 4.0, "cross_fill", "#ffffff"), "no LSOA match"
                )
            ],
        )
    )
    disagree = labels(
        layer(
            "check_disagreements_2025",
            "Check: NHSPD vs spatial LSOA disagreements (Aug 2025, fixed by ONS in 2026)",
        ),
        "postcode || '  ' || lsoa_reference || ' -> ' || lsoa_spatial",
        size=7,
    )
    disagree.setRenderer(
        QgsCategorizedSymbolRenderer(
            "nhspd_release",
            [
                QgsRendererCategory(
                    "2025-08", marker("#ffd700", 5.0, "star", "#333333"), "disagreement in Aug 2025"
                )
            ],
        )
    )
    add(
        "Checks (silver: spatial QA on public postcodes)",
        [nomatch, disagree],
        {nomatch.name(), disagree.name()},
    )

    # ---------------- inputs
    pcs = scale_limit(
        categorized(
            layer("in_postcodes", "Postcodes, NHSPD Aug 2026 (zoom in)"),
            "is_live",
            [
                (True, marker("#3a6ea5", 1.4, stroke="#ffffff"), "live"),
                (False, marker("#9a9a9a", 1.2, stroke="#ffffff"), "terminated"),
            ],
        ),
        60000,
    )
    census = scale_limit(
        graduated(
            layer("in_postcodes", "Census 2021 residents per postcode (zoom in)"),
            "census_residents",
            [
                (0, 0.5, "#dddddd", "none"),
                (0.5, 20, "#fdd49e", "1 - 19"),
                (20, 50, "#fc8d59", "20 - 49"),
                (50, 100, "#d7301f", "50 - 99"),
                (100, 100000, "#7f0000", "100 and over"),
            ],
            symbol=lambda c: marker(c, 1.8, stroke=c),
            opacity=1.0,
        ),
        60000,
    )
    ods = labels(
        categorized(
            layer("in_ods_organisations", "ODS organisations: GP practice (RO76) or not"),
            "is_gp_practice",
            [
                (True, marker("#1a9850", 2.6, stroke="#ffffff"), "GP practice (RO76): holds a list"),
                (
                    False,
                    marker("#d73027", 3.0, "triangle", "#ffffff"),
                    "other: minor injury unit, prison, pharmacy...",
                ),
            ],
        ),
        "if(is_gp_practice, '', practice_name)",
        size=7,
        max_scale=150000,
    )
    lsoa = labels(
        layer("in_lsoa21", "LSOA 2021 boundaries (study area)"), "lsoa21_name", size=7, max_scale=40000
    )
    lsoa.setRenderer(QgsSingleSymbolRenderer(outline("#555555", 0.15)))
    msoa_b = QgsVectorLayer(f"{GPKG}|layername=in_msoa21", "MSOA 2021 boundaries (study area)", "ogr")
    msoa_b.setRenderer(QgsSingleSymbolRenderer(outline("#7a3e9d", 0.35)))
    icb26 = labels(layer("in_icb_2026", "ICB boundaries, April 2026 edition"), "icb_name", size=8)
    icb26.setRenderer(QgsSingleSymbolRenderer(outline("#c51b7d", 0.6)))
    icb23 = labels(layer("in_icb_2023", "ICB boundaries, April 2023 edition"), "icb_name", size=8)
    icb23.setRenderer(QgsSingleSymbolRenderer(outline("#4d9221", 0.6, "dash")))
    add("Inputs (reference data)", [ods, pcs, census, lsoa, msoa_b, icb26, icb23], {ods.name()})

    # ---------------- basemap
    osm = QgsRasterLayer(
        "type=xyz&url=https://tile.openstreetmap.org/{z}/{x}/{y}.png&zmax=19&zmin=0", "OpenStreetMap", "wms"
    )
    project.addMapLayer(osm, False)
    root.addGroup("Basemap").addLayer(osm)

    extent = real.extent()
    extent.grow(2000)
    project.viewSettings().setDefaultViewExtent(
        __import__("qgis.core", fromlist=["QgsReferencedRectangle"]).QgsReferencedRectangle(
            extent, project.crs()
        )
    )
    return extent


def render(project: QgsProject, extent: QgsRectangle, path: Path) -> None:
    visible = list(project.layerTreeRoot().checkedLayers())
    ms = QgsMapSettings()
    ms.setLayers(visible)
    ms.setDestinationCrs(project.crs())
    ms.setExtent(extent)
    ms.setOutputSize(QSize(1400, 1100))
    ms.setBackgroundColor(QColor("white"))
    job = QgsMapRendererParallelJob(ms)
    job.start()
    job.waitForFinished()
    job.renderedImage().save(str(path))


def main() -> int:
    app = QgsApplication([], False)
    app.initQgis()
    project = QgsProject.instance()
    project.setFilePathStorage(__import__("qgis.core", fromlist=["Qgis"]).Qgis.FilePathType.Relative)
    extent = build(project)
    ok = project.write(str(OUT))
    print(("wrote " if ok else "FAILED to write ") + str(OUT))
    if "--render" in sys.argv:
        render(project, extent, HERE / "preview.png")
        print("wrote", HERE / "preview.png")
    app.exitQgis()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
