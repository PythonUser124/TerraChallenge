# TerraChallenge

## Applying git patches

1. Save the patch file (often with a `.patch` or `.diff` extension) to your working directory.
2. Review the patch with `git apply --stat <patch-file>` to see which files will change, and `git apply --check <patch-file>` to confirm it can be applied cleanly.
3. Apply the patch using `git apply <patch-file>`. If you want the changes staged automatically, run `git apply --index <patch-file>` instead.
4. Inspect the result with `git status` and `git diff` to verify the updates, then commit as usual.

If the patch does not apply cleanly, try updating your branch (for example, `git pull --rebase`) or applying it with `git am <patch-file>` when the patch was generated from commits with `git format-patch`.
