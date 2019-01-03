import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# Parse the version
with open('bcdata/__init__.py', 'r') as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            break

# Get the long description from the relevant file
with open('README.rst', encoding='utf-8') as f:
    long_description = f.read()


setup(name='bcdata',
      version='0.0.1',
      description=u"Python shortcuts for DataBC WFS",
      long_description=long_description,
      classifiers=[],
      keywords='',
      author=u"Simon Norris",
      author_email='snorris@hillcrestgeo.ca',
      url='https://github.com/smnorris/bcdata',
      license='Apache',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=read('requirements.txt').splitlines(),
      extras_require={
          'test': ['pytest'],
      },
      entry_points="""
      [console_scripts]
      bcdata=bcdata.cli:cli
      """
      )
