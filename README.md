I was looking around through the `pdl-person-build` repo and saw some opportunity for improvement, specifically around how function dependencies are handled.

The current method appears to be simply keeping track of which methods have been called and comparing that list to the current function's dependency list.

The only enforcement done is to throw an exception if a function is called out of order, so the programmer effectively has to declare the dependency twice (once in the decorator, and again by explicitly calling the function in the correct sequence).

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
```

Cost
----
The decorators are basically one-to-one, so the bulk of the code change would be:
1. replacing decorators
2. deleting explicit function calls in lieu of `task.run()`.
3. including new library

I did a quick grep through the source and it appears this decorator
is only used 45 times, so I estimate this would take less than a day
for one person to implement.

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
ensure proper execution order vs calling `task.run()`, the benefit becomes more apparent.

