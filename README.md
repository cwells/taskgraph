Problem
-------
I was looking around through the `pdl-person-build` repo and saw some opportunity for improvement, specifically around how function dependencies are handled.

The current method appears to be simply keeping track of which methods have already been called, comparing that list to the current function's dependency list, and throwing an exception if there's a dependency missing
from the completed list.

As such, the programmer effectively has to declare the dependency twice (once in the decorator, and again by explicitly calling the function in the correct sequence later on). Worse, these dependencies are generally declared far apart from each other in the code.

I've written a small PoC showing how calculating the dependency tree in advance provides several benefits:

https://gist.github.com/cwells/b8271d139742f2d45ef481bddad76ddd

Benefits
--------
1. programmer only needs to declare dependencies once, in the decorator
2. less overhead per function call
3. parallelization potential for non-interdependent functions

Current state
-------------
This is an example from the current code:
```python
@dependent_on(validate_fields)
def clean_fields_pre_redis(self):
```

The programmer here declares the dependency `validate_fields -> clean_fields_pre_redis`, but then has to explicitly re-declare
that same relationship later on by calling them in order:

```python
def run(self):
    self.validate_fields()
    self.clean_fields_pre_redis()
    ...
```

Further, functions must utilize a second decorator to keep the completed list updated:

```python
@decorate_all_methods(record_function_completion)
class PersonPostProcessor(object):
```

Proposed state
--------------
```python
task = TaskGraph()

@task.requires(validate_fields)
def clean_fields_pre_redis(self):
```

```python
def run(self):
    task.run(self)
```
One class, one decorator, one function.

Cost
----
The decorators are basically one-to-one, so the bulk of the code change would be:
1. replacing decorators
2. deleting explicit function calls in lieu of `task.run()`.
3. including new library (toposort)

I did a quick grep through the source and it appears this decorator
is only used 45 times, and this would appear to be a largely mechanical
refactor, so I estimate this would take one day to complete.

While it may seem like a rather small thing, when presented with this:

```python
    def run(self):
        self.set_trusted_label()
        self.drop_merged_untrusted_records()

        self.apply_version_hotfixes()
        self.set_source_structure()
        self.set_record_id()
        self.set_field_defaults()

        self.clean_fields_pre_redis()
        self.set_inferred_skills_pre_redis()
        self.execute_redis_pipeline()
        self.clean_fields_post_redis()
        self.set_inferred_skills_post_redis()

        self.finalize_experience()
        self.finalize_education()
        self.finalize_locations()
        self.finalize_emails()
        self.finalize_phones()
        self.finalize_profiles()
        self.finalize_names()
        self.finalize_gender()
        self.finalize_birth_date()
        self.finalize_industries()

        self.split_exposed_pii()
        self.split_mobile_phones()

        self.check_is_frankenstein()
        self.check_is_whole()
        self.extend_id_suffix()

        self.move_current_professional_emails()
        self.add_inferred_work_emails()
        self.add_back_valid_b2b_work_emails()

        self.add_opt_out_status()
        self.add_linkedin_connections()
        self.add_inferred_salary()
        self.add_inferred_years_experience()
        self.add_email_name_variations()
        self.add_stats()
        self.add_inferred_location_names()
        self.add_email_hashes()
        self.add_employment_status()
        self.add_datapull_categories()
        self.add_random()

        return self.person
```

and realizing that this must be hand-curated by the developer to
ensure proper execution order vs simply declaring dependencies in the decorator and calling `task.run()`, the benefit becomes more apparent.


Additional opportunities
------------------------
In addition to this, many of the classes leverage the pattern of modifying shared mutable state. Methods should be refactored to return values
rather than directly modifying a shared object.

Conceptually, there's no difference between class-scoped shared mutable state and globally-scoped shared mutable state. The same issues that
plague globally-scoped mutable state are also present with class-scoped mutable state; they differ only in scale. Shared mutable state
is an anti-pattern that should be avoided. It makes parallelization impossible, hides data changes in side-effects, and generally encourages
write-only code.

Refactoring the methods to be decorated with `task.requires` to be more functional would likely allow for parallelization of some portion of
the current code.
