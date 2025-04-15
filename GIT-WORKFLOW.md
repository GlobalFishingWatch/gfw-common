[Git Flow]: https://nvie.com/posts/a-successful-git-branching-model/
[Semantic Versioning]: https://semver.org

> [!IMPORTANT]
In the following, **MAJOR**, **MINOR** and **PATCH** refer to [Semantic Versioning].

We use [Git Flow] as our branching strategy, which is well suited for projects with long release cycles.
Here we present a summary of the strategy.


## **Permanent branches**:

| Branch         | Purpose                                                                   |
|----------------|---------------------------------------------------------------------------|
| `main`         | Represents the production-ready state; all releases originate here.       |
| `develop`      | The integration branch for ongoing development; features are merged here. |

In addition to the permanent branches, auxiliary branches are used to develop features, releases and hotfixes.

## **Auxiliary branches**:

Branch names should be descriptive, lowercase and with words separated by hyphens "-".

Optionally, branch names can be prefixed with JIRA ticket, e.g., `PIPELINE-2020-name-of-the-branch`.

## **Feature branches**:

1. Create a branch from **develop**.
2. Work on the feature. Try to minimize size of commits.
3. Rebase on-top of **develop**.
4. Push changes and open a PR. Ask for a review.
5. Merge back to **develop** with a merge commit.

<div align="justify">

To maintain a clear _semi-linear_ history in **develop**,
we rebease feature branches on top of **develop** before merging.
The merge should be done **forcing a merge commit**,
otherwise would be a fast-forward merge (because we rebased)
and the history would be linear instead of semi-linear,
losing the context of the branch.
This is enforced in the GitHub UI,
but locally is done with:
```shell
git merge branch --no-ff
```
</div>

## **Release branches**:

1. Create a branch named **release/x.y.z** from **develop**.
2. Perform all steps needed to make the release.
3. Push changes and open a PR. Ask for a review.
4. Merge to **main** and **develop**.
5. Tag **main**.

## **Hotfix branches**:

These are quick **PATCH** increments done only to current **MAJOR** release:

1. Create a branch named **hotfix/x.y.z** from **main**.
2. Work on the fix. Perform steps needed to make the release.
3. Push changes and open a PR. Ask for a review.
4. Merge back to **main** and also to **develop**.
5. Tag **main**.
