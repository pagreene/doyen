from setuptools import setup, find_packages

setup(
    name="doyen_ingestion",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    entry_points={"console_scripts": ["doyen-ingest = doyen.pubmed_processor:main"]},
    python_requires=">=3.9",
    install_requires=[
        "elasticsearch",
        "indra @ https://github.com/pagreene/indra/archive/get-citations.zip",
    ],
    extras_require={"dev": ["pytest", "black"]},
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
    ],
)
