let g:ale_linters = {'cpp': ['clangtidy', 'cpplint']}
let g:ale_cpp_clangtidy_checks = [
  \'-bugprone-branch-clone',
  \'-bugprone-signed-char-misuse',
  \'-bugprone-unhandled-self-assignment',
  \'-clang-diagnostic-implicit-int-float-conversion',
  \'-modernize-use-auto',
  \'-modernize-use-trailing-return-type',
  \'-readability-convert-member-functions-to-static',
  \'-readability-make-member-function-const',
  \'-readability-qualified-auto',
  \'-readability-redundant-access-specifiers'
  \]
let g:ale_fixers = {'cpp': ['clang-format']}
