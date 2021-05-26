from setuptools import setup, find_packages


setup(
    name='henri_libs',
    description='Henris Libraries',
    version='0.0.1',
    author='Henri Branken',
    packages=find_packages(include=['custom_udfs', 'pyspark_stats', 'snowflake_basics', 'cvm_activity']),
    license='MatogenAI',
    install_requires=['DateTime==4.3', 'numpy==1.20.3', 'py4j==0.10.9', 'pyspark==3.1.1',
                      'python-dateutil==2.8.1', 'pytz==2021.1', 'six==1.14.0',
                      'zope.interface==5.4.0']
)
