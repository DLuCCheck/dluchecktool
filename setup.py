from setuptools import setup, find_packages # type: ignore


setup(name='dlucheck',
      version='0.1',
      url='https://github.com/DLuCCheck/dluchecktool',
      license='MIT',
      author='Demola Team',
      packages=find_packages(),
      install_requires=[
            'numpy',
            'pysqlite3',
            'openpyxl',
            'pyxlsb',
            'pandas'
      ],
      description='Table serialization and crosschecking',
      classifiers=[])
