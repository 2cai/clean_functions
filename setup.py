
import setuptools

#with open("README.md", "r") as fh:
#    long_description = fh.read()

setuptools.setup(
    name="clean_functions",
    version="1.0.0",
    author="...",
    author_email="...",
    description="Pre-trained language models forthe  Brazilian  legal  language.",
    #long_description=long_description,
    #long_description_content_type="text/markdown",
    url='https://github.com/2cai/brain_lib',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['pandas', 'datetime','operator','IPython','matplotlib']
) 