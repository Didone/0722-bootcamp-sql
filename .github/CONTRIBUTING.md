# How to contribute

First of all, thank you for taking the time to contribute to this project. We've tried to make a stable project and try to fix bugs and add new features continuously. You can help us do more.

## :innocent: Code of Conduct

This project and everyone participating in it is governed by the [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to any member of our [**administration team**](@Didone).

## :octocat: How to getting started

We recommend to install the following tools before start:

- [Python](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/)

### Environments

After clone this repository, use [Poetry](https://python-poetry.org/) to create your virtual environment. And install all dependencies.
If you are participating in one of our bootcamps use the `-E bootcamp` option to install all the optional packages.

```bash
# Activate your virtual environment
poetry shell
# Install all dependencies
poetry install -E bootcamp
```

### Writing code

Contributing to a project on Github is pretty straight forward. If this is you're first time, these are the steps you should take.

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Commit to your fork using clear [commit messages](#git-commit-messages).
4. Send us a pull request, answering any default questions in the pull request interface.
5. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

That's it! Read the code available and change the part you don't like! You're change should not break the existing code and should pass the tests.

If you're adding a new functionality, start from the branch **main**. It would be a better practice to create a new branch and work in there.

When you're done, submit a pull request and for one of the maintainers to check it out. We would let you know if there is any problem or any changes that should be considered.

> GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and [creating a pull request](https://help.github.com/articles/creating-a-pull-request/).

### Running Tests

Use `pytest` framework to write and run your tests

```bash
pytest --pdb
```

```log
================================== test session starts ===================================
platform linux -- Python 3.8.10, pytest-5.4.3, py-1.11.0, pluggy-0.13.1
rootdir: bootcamp/sql/csvms
collected 23 items                                                                        

tests/test_joins.py .....                                                           [ 21%]
tests/test_operations.py ..........                                                 [ 65%]
tests/test_project.py ...                                                           [ 78%]
tests/test_schema.py .....                                                          [100%]

=================================== 6 passed in 0.23s ====================================
```

> Check more information about our [continuous integration](../docs/Continuous%20Integration.md) worfkow

### Documentation

Every chunk of code that may be hard to understand has some comments above it. If you write some new code or change some part of the existing code in a way that it would not be functional without changing it's usages, it needs to be documented.

### Code conventions

- [Python](https://www.python.org/dev/peps/pep-0008/)
  - Configure [Pylint](https://pylint.org/) in [your IDE](http://pylint.pycqa.org/en/latest/user_guide/ide-integration.html) to help you to follow this guides
- [Markdown](https://daringfireball.net/projects/markdown)

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Always reference issues and pull requests under commit message description
- Consider starting the commit message with an applicable emoji:
  - :art: `:art:` when improving the format/structure of the code
  - :racehorse: `:racehorse:` when improving performance
  - :memo: `:memo:` when writing docs
  - :bug: `:bug:` when fixing a bug
  - :fire: `:fire:` when removing code or files
  - :green_heart: `:green_heart:` when fixing the CI build
  - :white_check_mark: `:white_check_mark:` when adding tests
  - :lock: `:lock:` when dealing with security
  - :arrow_up: `:arrow_up:` when upgrading dependencies
  - :arrow_down: `:arrow_down:` when downgrading dependencies
  - :shirt: `:shirt:` when removing linter warnings

## Licensing

See the [LICENSE](../LICENSE) file for our project's licensing.

We may ask you to sign a [Contributor License Agreement (CLA)](https://en.wikipedia.org/wiki/Contributor_License_Agreement) for larger changes.

## Attribution

This document is adapted from:

- <https://mozillascience.github.io/working-open-workshop/github_for_collaboration>
- <https://github.com/atom/atom/blob/master/CONTRIBUTING.md>
- <https://gist.github.com/mpourismaiel/6a9eb6c69b5357d8bcc0>