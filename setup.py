from setuptools import setup, find_packages

setup(name="Tan", 
      version="1.0.0", 
      python_requires=">=3.8",
      packages=find_packages(include=["data", "data.*"]),
      install_requires=[
          'pandas>=2.0',
          'SQLAlchemy>=2.0',
          'psycopg2-binary>=2.0',
          'pytest>=7.0'
      ]
)
