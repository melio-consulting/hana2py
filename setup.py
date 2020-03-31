from setuptools import setup

setup(name='hana2py',
      version='0.2',
      packages=['hana2py'],
      license='MIT',
      # long_description=open('README.md').read(),
      description='A simple library for connecting SAP Hana and Pandas.',
      author='Merelda Wu',
      author_email='merelda@melio.co.za',
      install_requires=[
            'pyodbc', 
            'python-dotenv',
            'pympler',
            'sqlalchemy-hana',
            'numpy',
            'pandas',
            'tqdm'
      ],
      test_suite='nose.collector',
      tests_require=['nose'],
      include_package_data=True,
      zip_safe=False)
