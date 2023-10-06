from setuptools import setup, find_namespace_packages

setup(name='nba.stats',
      version='0.0.1',
      package_dir={'': 'src'},
      packages=find_namespace_packages(where='src'),
      description='NBA Stats Collector and Analyzer.',
      author='Christian Martinez',
      license='private',
      test_suite='build_tests',
      install_requires=[
          'caritas.depot-core',
          'psycopg2',
          'beautifulsoup4',
          'requests',
          'scikit-learn',
          'python-dateutil'
      ],
      zip_safe=False
      )
