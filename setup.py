"""Setup script for the distributed key-value store"""

from setuptools import setup, find_packages

setup(
    name="kvstore-raft",
    version="1.1.1",
    description="Distributed Key-Value Store with Raft Consensus",
    author="S0meR4nd0mGuy",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=[
        "pytest>=7.4.0",
        "pytest-asyncio>=0.21.0",
        "python-dotenv>=1.0.0",
        "advanced-logging>=1.0.0",
    ],
    extras_require={
        "dev": [
            "black>=23.7.0",
            "flake8>=6.0.0",
            "mypy>=1.4.1",
        ],
        "grpc": [
            "grpcio>=1.57.0",
            "grpcio-tools>=1.57.0",
        ],
        "api": [
            "fastapi>=0.100.0",
            "uvicorn>=0.23.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "kvstore-node=src.main:main",
            "kvstore-cli=src.client.cli:main",
        ],
    },
)