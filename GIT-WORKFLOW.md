# Git workflow summary:

[Git Flow]: https://nvie.com/posts/a-successful-git-branching-model/
[Semantic Versioning]: https://semver.org

> [!IMPORTANT]
In the following, **X**, **Y** and **Z** refer to **MAJOR**, **MINOR** and **PATCH** of [Semantic Versioning].

We use [Git Flow] as our branching strategy,
which is well suited for projects with long release cycles.
In this document we present a summary of the strategy.

These are the 5 types of branches used in this strategy:
| Name            | Type      | Purpose                                                                          |
|-----------------|-----------|----------------------------------------------------------------------------------|
| `main`          | Permanent | Represents the production-ready state; all releases originate here.              |
| `develop`       | Permanent | The integration branch for ongoing development; features are merged here.        |
| `feature/*`     | Temporary | Branches for developing new features, branched off from `develop`.               |
| `release/X.Y.Z` | Temporary | Branches for preparing a new production release, branched off from `develop`.    |
| `hotfix/*`      | Temporary | Branches for critical fixes to the production version, branched off from `main`. |

<div align="justify">

## **Temporary branches**:

Temporary branches are used to integrate features, releases and hotfixes to the permanent branches.
Names should be descriptive and concise, all lowercase and with words separated by hyphens "-".
Optionally, feature branch names can be prefixed with JIRA ticket instead of the `feature/` prefix.
For example, you can use something like `PIPELINE-2020-name-of-the-branch`.

### **Feature workflow**:

1. Create a branch from `develop`.
2. Work on the feature.
3. Rebase on-top of `develop`.
4. Push changes and open a PR. Ask for a review.
5. Merge branch to `develop` with a merge commit.


To maintain a clear _semi-linear_ history in `develop`,
we rebase feature branches on top of `develop` before merging.
The merge should be done **forcing a merge commit**,
otherwise would be a fast-forward merge (because we rebased)
and the history would be linear instead of semi-linear,
losing the context of the branch.
This is enforced in the GitHub UI,
but locally is done with:
```shell
git checkout develop
git pull
git merge branch_name --no-ff
```

### **Release workflow**:

1. Create a branch named `release/X.Y.Z` from `develop`.
2. Perform all steps needed to make the release.
3. Push changes and open a PR. Ask for a review.
4. Merge `release/X.Y.Z` to `main` and also to `develop`.
5. Create a release from `main`. The tag should be named `vX.Y.Z`.

### `Hotfix workflow`:

1. Create a branch named `hotfix/your-branch-name` from `main`.
2. Work on the fix. Perform steps needed to make the release.
3. Push changes and open a PR. Ask for a review.
4. Merge `hotfix/your-branch-name` to `main` and also to `develop`.
5. Create a release from `main`. The tag should be named `vX.Y.Z`.


## Pull Request Checklist

When submitting a pull request, please ensure it meets the following criteria:
-  The PR targets the correct base branch (`develop` for features, `main` for hotfixes targeting a specific release).
-  The title and body provide a clear and concise explanation of **what** the PR does and **why** it's necessary.
-  Documentation has been updated or created if the PR introduces new features or changes existing behavior.
-  Tests have been added for new features or bug fixes to ensure they function correctly and prevent regressions.

</div>
