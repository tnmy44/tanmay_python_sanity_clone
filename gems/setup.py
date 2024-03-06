from setuptools import setup, find_packages
packages_to_include = find_packages(exclude = ['test.*', 'test', 'test_manual'])
setup(
    name = 'abhishekse2etestsprophecyio_team_pythonproject',
    version = '3.3.21',
    packages = packages_to_include,
    description = '',
    install_requires = [
'gensim==4.3.2', 'numerizer==0.2.3', 'pendulum==2.1.2', ],
    data_files = ["resources/extensions.idx"]
)
