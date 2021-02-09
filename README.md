# Criteria-ETL-Template

Criteria-ETL-training is used for introducing as template for every new Criteria ETL project.

## `notebooks` directory
Used for storing all notebooks of the project.

## `data` directory
As usual, the data for Prosperia Social projects, is available in [Google Drive](https://drive.google.com/drive/folders/1xz-fulc2uUFf2JIEfoM0UQyI2TQyCb8t?usp=sharing) at the repository homonym folder, and NOT at the git repository.

## `src` directory
Used for `project-etl` library, containing configuration files and local modules. It is installed in editable mode via `pip` in the `environment.yml` file


## User guide
1. Open `environment.yml` and change the name of the enviroment. We recommend to call it "criteria-<ISO 3166-1 alpha-2 country code in lowercase>", e.g. "criteria-br", "criteria-uy", "citeria-sv". ISO 3166-1 alpha-2 country codes are available [here](https://www.iso.org/obp/ui/#search)

2. Open your terminal at this folder and run:
    ```bash
    conda env create -f environment.yml
    ```
    this will create the environment as specified by the `yml` file. Note that this installs all the packages specified at `requirements.txt` as well as the packages present in `src` folder in ***editable*** mode via pip.  
3. Excecute the `set-up.ipynb` in the `notebook` directory. If it successfully runs, the project is ready to use.
4. Create a repository on the [organizational github account](https://github.com/prosperia-social). We usually call the projects "criteria-<country name>", e.g. "criteria-brasil", "criteria-uruguay", "criteria-salvador".
5. Change git remote `origin` to the repository created on the previous step:
   ```bash
   git remote set-url origin <remote_url>
   ```
   We recommend using SSH protocol on the `<remote_url>` to skip passphrases (see [GitHub Guide](https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh)).

