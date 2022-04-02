# ```excavator```: A simple script for scraping options data

```excavator``` is a script I threw together to pull daily options data for backtesting purposes. It's intended to be run from the command line and uses a .env configuration to store your exchange credentials.

To get started using ```excavator```, run the following in your terminal:

```python
# Clone excavator
git clone https://github.com/pattertj/excavator.git

# Install dependencies
pipenv install

# Run excavator
pipenv run excavator/__main__.py
```

To get up and running for development, additionally run:

```python
# Install dependencies
pipenv install --dev

# Setup pre-commit and pre-push hooks
pipenv run pre-commit install -t pre-commit
pipenv run pre-commit install -t pre-push
```
