from setuptools import setup, find_packages
import setuptools.command.egg_info as egg_info_cmd
import os

try:
    import gittaggers
    tagger = gittaggers.EggInfoFromGit
except ImportError:
    tagger = egg_info_cmd.egg_info

setup(
    name='biowardrobe_analysis',
    description='Biowardrobe analysis',
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    version='0.0.1dev1',
    url='https://github.com/Barski-lab/biowardrobe_basic_analysis',
    download_url=('https://github.com/Barski-lab/biowardrobe_basic_analysis'),
    author='Michael Kotliar',
    author_email='misha.kotliar@gmail.com',
    license = 'Apache 2.0',
    packages=find_packages(),
    install_requires=[
        'testing.mysqld',
        'regex'
    ],
    zip_safe=False,
    cmdclass={'egg_info': tagger}
)