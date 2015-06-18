from setuptools import setup

setup(
    name = "findpg",
    version = "3.0",
    author = "Cecile Tonglet",
    author_email = "cecile.tonglet@gmail.com",
    description = ("Find the best suited version of PostgreSQL to restore a "
                   "dump"),
    license = "MIT",
    keywords = "postgres database dump restore",
    url = "https://github.com/cecton/findpg",
    py_modules=['findpg'],
    scripts=['findpg.py'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
    ],
)
