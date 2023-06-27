Converting vector data
======================

.. jupyter-execute::

    from matplotlib import pyplot
    from pathlib import Path
    import os

    # increase the plot size
    pyplot.rcParams['figure.dpi'] = 120

    project_root = Path(os.environ["PROJECT_ROOT"])


.. jupyter-execute::

    import geopandas as gpd

    world = gpd.read_file(project_root / "data/naturalearth_110m_admin_0_countries.fgb")
    africa = world[world["CONTINENT"] == "Africa"]
    africa.plot(column="NAME_EN")


Converting a complete `GeoDataFrame` to cells
---------------------------------------------

Includes building a new `GeoDataFrame` with the cell geometries using :py:func:`h3ronpy.pandas.geodataframe_to_cells`:

.. jupyter-execute::

    from h3ronpy.pandas.vector import geodataframe_to_cells, cells_dataframe_to_geodataframe

    df = geodataframe_to_cells(africa, 3)
    gdf = cells_dataframe_to_geodataframe(df)
    gdf.plot(column="NAME_EN")


Polygon fill modes
------------------

Polygon to H3 conversion is based on centroid containment.
Depending on the shape of the geometry the resulting cells may look like below:


.. jupyter-execute::

    namibia = africa[africa["NAME_EN"] == "Namibia"]

    def fill_namibia(**kw):
        cell_ax = cells_dataframe_to_geodataframe(geodataframe_to_cells(namibia, 3, **kw)).plot()
        return namibia.plot(ax=cell_ax, facecolor=(0,0,0,0), edgecolor='black')

    fill_namibia()

The `all_intersecting` argument extends this to include all cells which intersect with the polygon geometry:

.. jupyter-execute::

    fill_namibia(all_intersecting=True)


Merging cells into larger polygons
----------------------------------

.. jupyter-execute::

    from h3ronpy.pandas.vector import cells_to_polygons, geoseries_to_cells

    gpd.GeoDataFrame({
        "geometry": cells_to_polygons(geoseries_to_cells(namibia.geometry, 3, flatten=True), link_cells=True)
    }).plot()


Single geometries
-----------------

It is also possible to convert single `shapely` geometries or any other type providing the python `__geo_interface__`:

.. jupyter-execute::

    from h3ronpy.pandas.vector import geometry_to_cells

    namibia_geom = namibia["geometry"].iloc[0]
    print(namibia_geom)
    geometry_to_cells(namibia_geom, 3)


