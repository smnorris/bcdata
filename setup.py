import os

from setuptools import find_packages, setup


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

setup(
    name="bcdata",
    version=version,
    description="Python tools for quick access to DataBC geo-data available via WFS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Utilities",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    keywords='gis geospatial data BC DataBC download "Britsh Columbia"',
    author="Simon Norris",
    author_email="snorris@hillcrestgeo.ca",
    url="https://github.com/smnorris/bcdata",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=read("requirements.txt").splitlines(),
    extras_require={"test": ["pytest>=3", "pre-commit", "requests_mock"]},
    entry_points="""
      [console_scripts]
      bcdata=bcdata.cli:cli
      """,
)
