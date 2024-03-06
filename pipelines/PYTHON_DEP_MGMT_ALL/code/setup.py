from setuptools import setup, find_packages
setup(
    name = 'REL_PY_PIP_DEP_MGMT_ALL',
    version = '1.0',
    packages = (
      find_packages(include = ('com.main1.pythondepmanagement_1*', ))
      + ['prophecy_config_instances.com.main1.configall.all_the_configs']
    ),
    package_dir = {
      'prophecy_config_instances.com.main1.configall.all_the_configs': 'configs/resources/com/main1/configall/all_the_configs'
    },
    package_data = {'prophecy_config_instances.com.main1.configall.all_the_configs' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = ['pandas>=1.4.2', 'scipy>=1.6.3,<=1.8.1', 'matplotlib==3.5.2', 'requests~=2.28.0', 'Theano==1.0.5', 'gensim==4.3.2',
     'numerizer==0.2.3', 'pendulum==2.1.2', 'prophecy-libs==1.8.8'],
    entry_points = {
'console_scripts' : [
'main = com.main1.pythondepmanagement_1.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
