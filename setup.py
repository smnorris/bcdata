from codecs import open as codecs_open
from setuptools import setup, find_packages


# Get the long description from the relevant file
with codecs_open('README.rst', encoding='utf-8') as f:
    long_description = f.read()


setup(name='bcdata',
      version='0.0.1',
      description=u"Data BC Distribution Service, automated",
      long_description=long_description,
      classifiers=[],
      keywords='',
      author=u"Simon Norris",
      author_email='snorris@hillcrestgeo.ca',
      url='https://github.com/smnorris/bcdata',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'click',
          'requests',
          'selenium',
          'polling'
      ],
      extras_require={
          'test': ['pytest', 'fiona'],
      },
      entry_points="""
      [console_scripts]
      bcdata=bcdata.scripts.cli:cli
      """
      )
