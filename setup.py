import os

from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


# Parse the version
with open("bcdata/__init__.py", "r") as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            break

# Get the long description from the relevant file
with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

requirements = [
    "click>=7.0,<8",
    "cligj>=0.5",
    "pgdata>=0.0.12",
    "requests",
    "owslib",
    "rasterio",
    "geopandas",
]


setup(
    name="bcdata",
    version=version,
    description=u"Python tools for quick access to DataBC geo-data available via WFS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Utilities",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    keywords='gis geospatial data BC DataBC download "Britsh Columbia"',
    author=u"Simon Norris",
    author_email="snorris@hillcrestgeo.ca",
    url="https://github.com/smnorris/bcdata",
    license="Apache",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=requirements,
    extras_require={"test": ["pytest>=3", "rasterio"]},
    entry_points="""
      [console_scripts]
      bcdata=bcdata.cli:cli
      """,
)
