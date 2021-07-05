# no-maintain-checks
"""
This module is dervied from:

Cryno: Data Profiling Library
https://github.com/joocer/cryno

(C) 2021 Justin Joyce.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

┌──────────────────────────────────────────────────────────────────────────────┐
│          NOTE: BINNING HAS BEEN REMOVED AS BOTH SLOW AND INACCURATE          │
└──────────────────────────────────────────────────────────────────────────────┘

"""
from ..flows.internals.base_operator import BaseOperator
from ..data.formats import json
from ..data.formats.dictset.display import draw_histogram_bins
from ..index.bloom_filter import BloomFilter

ACCURATE_THRESHOLD = 5000
BLOOM_FILTER_SIZE = 2500000


class ProfileDataOperator(BaseOperator):
    def __init__(self, schema=None, inner_writer=None, **kwargs):
        """
        ProfileData Operator is designed to profile streaming data this is done
        with some approximation, especially when summarizations would require
        all of the values such as counting unique values or binning values.

        It starts summarizing from 5,000 values, small datasets will be more
        accurate. Some measures will be pretty much useless the closer it gets
        to 2,500,000 records.

        These limits are in place to reduce memory overheads, even with the
        approaches taken this is pretty memory hungry.
        """
        super().__init__(**kwargs)

        self.inner_writer = inner_writer
        self.fields = schema._validators

        self.summary = {}
        for field in self.fields:
            self.summary[field] = {
                "nulls": 0,
                "items": 0,
                "type": ProfileDataOperator.get_type(schema._validators[field]),
            }
        self.logger.warning(
            "`ProfileDataOperator` is an alpha feature and may change in the future."
        )

    def execute(self, data, context):

        for field in self.fields:
            field_summary = self.summary[field]
            field_summary["items"] += 1
            field_type = field_summary["type"]
            field_value = data.get(field)
            if field_value is None:
                field_summary["nulls"] += 1

            elif field_type == "numeric":
                if isinstance(field_value, str):
                    field_value = float(field_value)
                field_summary = ProfileDataOperator.profile_numeric_data(
                    field_summary, field_value
                )

            elif field_type == "date":
                # convert to epoch seconds and then just treat as a number
                from dateutil import parser

                field_value = int(parser.isoparse(field_value).timestamp())
                field_summary = ProfileDataOperator.profile_numeric_data(
                    field_summary, field_value
                )

            elif field_type == "string":
                field_summary = ProfileDataOperator.profile_string_data(
                    field_summary, field_value
                )

            elif field_type == "enum":
                """
                Profiler for enums (including boolean), just counts the
                number of values in each category
                """
                if field_summary.get("values") is None:
                    field_summary["values"] = {}
                field_summary["values"][f"{field_value}"] = (
                    field_summary["values"].get(f"{field_value}", 0) + 1
                )

        return data, context

    def finalize(self, context: dict = None):

        if not context:
            context = {}

        for k in [k for k in self.summary if self.summary[k]["type"] == "string"]:

            if len(self.summary[k]["unique_values"]) < ACCURATE_THRESHOLD:
                # get rid of the unique values list
                self.summary[k]["unique_values"] = len(self.summary[k]["unique_values"])
                del self.summary[k]["approx_unique_values"]
            else:
                del self.summary[k]["unique_values"]

            del self.summary[k]["bloom_filter"]

        # rebin all of the binned data into a set of 10 bins
        # new_bins = [
        #    (k, ProfileDataOperator.redistribute_bins(self.summary[k]["bins"], 10))
        #    for k in self.summary
        #    if "bins" in self.summary[k]
        # ]
        # for k, bins in new_bins:
        #    self.summary[k]["bins"] = bins

        # convert date fields back to dates
        date_fields = [k for k in self.summary if self.summary[k]["type"] == "date"]
        for date_field in date_fields:
            self.summary[date_field]["min"] = ProfileDataOperator.date_from_epoch(
                self.summary[date_field].get("min", 0)
            )
            self.summary[date_field]["max"] = ProfileDataOperator.date_from_epoch(
                self.summary[date_field].get("max", 0)
            )
            # new_bins = {}  # type:ignore
            # for bound in self.summary[date_field].get("bins", []):
            #    bottom, top = bound
            #    bottom = ProfileDataOperator.date_from_epoch(bottom)
            #    top = ProfileDataOperator.date_from_epoch(top)
            #    new_bins[(bottom, top)] = self.summary[date_field][  # type:ignore
            #        "bins"
            #    ][bound]
            # self.summary[date_field]["bins"] = new_bins
            if self.summary[date_field].get("mean"):
                del self.summary[date_field]["mean"]
            if self.summary[date_field].get("cumsum"):
                del self.summary[date_field]["cumsum"]

        # change the bin names to strings from tuples
        # for k in [k for k in self.summary if "bins" in self.summary[k]]:
        #    new_bins = {}  # type:ignore
        #    old_bins = self.summary[k]["bins"]
        #    for ranges, v in old_bins.items():
        #        bottom, top = ranges
        #        label = f"{ProfileDataOperator.short_form(bottom)} to {ProfileDataOperator.short_form(top)}"
        #        new_bins[label] = v  # type:ignore
        #    self.summary[k]["bins"] = new_bins

        # put the profile into the context so it gets passed along
        context["mabel:profile"] = self.summary
        return context

    def __str__(self):
        def _inner():
            for field in self.fields:
                field_summary = self.summary[field]
                if field_summary["nulls"] == field_summary["items"]:
                    yield f"[---] {field:20} [count] {field_summary['items']} EMPTY"
                elif field_summary["type"] == "numeric":
                    yield f"[num] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%} [range] {ProfileDataOperator.short_form(field_summary.get('min'))} to {ProfileDataOperator.short_form(field_summary.get('max'))} [mean] {ProfileDataOperator.short_form(field_summary['mean'])} >{draw_histogram_bins([v for k,v in field_summary['bins'].items()])}<"
                elif field_summary["type"] == "date":
                    yield f"[dte] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%} [range] {field_summary.get('min')} to {field_summary.get('max')} >{draw_histogram_bins([v for k,v in field_summary['bins'].items()])}<"
                elif field_summary["type"] == "other":
                    yield f"[oth] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%}"
                elif field_summary["type"] == "string":
                    yield f"[str] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%} [unique] {field_summary['unique_values']} [longest] {field_summary['max_length']} [shortest] {field_summary['min_length']}"
                elif field_summary["type"] == "enum":
                    yield f"[enm] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%} {ProfileDataOperator.enum_summary(field_summary['values'])}"

        return "\n".join(_inner())

    def __repr__(self):
        return json.serialize(self.summary, indent=True)

    @staticmethod
    def redistribute_bins(bins, number_of_bins=100):
        """
        Redistribute binned data into fewer bins

        This results in items being put into incorrect bins, especially
        when the data is ordered.
        """
        mn = min([l for l, h in bins])
        mx = max([h for l, h in bins])

        bin_size = ((mx - mn) + 1) // number_of_bins

        new_bins = {}
        for counter in range(number_of_bins - 1):
            new_bins[
                (mn + (bin_size * counter), mn + (bin_size * (counter + 1)) - 1)
            ] = 0
        new_bins[(mn + (bin_size * (counter + 1)), mx)] = 0

        for old_bounds in bins:
            old_lower, old_upper = old_bounds
            old_mid = (old_lower + old_upper) // 2
            binned = False
            for new_bounds in new_bins:
                new_lower, new_upper = new_bounds
                if old_mid >= new_lower and old_mid <= new_upper:
                    new_bins[new_bounds] += bins[old_bounds]
                    binned = True
                    break
            if binned == False:
                print("miss", old_mid, mn, mx)

        return new_bins

    @staticmethod
    def get_type(validators):
        """
        Determine the data type so we can use the correct profiler
        """
        try:
            val = [str(v).split(".")[0].split(" ")[1] for v in validators]
            val = [v for v in val if not v in ["is_null", "is_other"]]
            val = val.pop()
        except (KeyError, ValueError, IndexError):
            val = "other"

        if val in ["is_numeric"]:
            return "numeric"
        if val in ["is_string", "is_cve"]:
            return "string"
        if val in ["is_valid_enum", "is_boolean"]:
            return "enum"
        if val in ["is_date"]:
            return "date"

        return "other"

    @staticmethod
    def date_from_epoch(seconds, form="%Y-%m-%d %H:%M:%S"):
        """
        Convert from milliseconds since epoch to a date string
        """
        import datetime

        return datetime.datetime.fromtimestamp(seconds).strftime(form)

    @staticmethod
    def profile_numeric_data(collector, value):
        """
        Profiler for numeric data, it collects:

        - minimim value
        - maximum value
        - cummulative sum
        - mean
        - approximate distribution

        This method take single value at a time to build the profile
        """
        if collector.get("max", value) <= value:
            collector["max"] = value
        if collector.get("min", value) >= value:
            collector["min"] = value
        collector["cumsum"] = collector.get("cumsum", 0) + value
        collector["mean"] = collector["cumsum"] / collector["items"]

        #        if collector.get("bins") is None:
        #            collector["bins"] = {}
        #        binned = False
        #        for bounds in collector["bins"]:
        #            bottom, top = bounds
        #            if value >= bottom and value <= top:
        #                collector["bins"][bounds] += 1
        #                binned = True
        #        if not binned:
        #            collector["bins"][(value, value)] = 1
        #
        #        if len(collector["bins"]) > 5007:  # first prime over 10000
        #            collector["bins"] = ProfileDataOperator.redistribute_bins(
        #                collector["bins"], 101
        #            )  # first prime over 100

        return collector

    @staticmethod
    def profile_string_data(collector, value):
        """
        Profiler for string data, it collects:

        - minimim length
        - maximum length
        - unique values (capped upper limit)

        This method take single value at a time to build the profile
        """
        if collector.get("max_length", len(value)) <= len(value):
            collector["max_length"] = len(value)
        if collector.get("min_length", len(value)) >= len(value):
            collector["min_length"] = len(value)
        if collector.get("bloom_filter") is None:
            collector["unique_values"] = []
            collector["bloom_filter"] = BloomFilter(
                number_of_elements=BLOOM_FILTER_SIZE, fp_rate=0.01
            )
            collector["approx_unique_values"] = 0

        # if we have only a few values, count them all
        if len(collector["unique_values"]) < ACCURATE_THRESHOLD:
            hashed = hash(value)
            if not hashed in collector["unique_values"]:
                collector["unique_values"].append(hashed)

        # when we have a lot of values, use a bloom filter
        if value not in collector["bloom_filter"]:
            collector["bloom_filter"].add(value)
            collector["approx_unique_values"] += 1

        return collector

    @staticmethod
    def short_form(num):
        if not isinstance(num, (int, float)):
            return num
        if isinstance(num, int) and abs(num) < 10000000:
            return str(num)
        return f"{num:.2f}"

    @staticmethod
    def enum_summary(dic):
        s = {
            k: v for k, v in sorted(dic.items(), key=lambda item: item[1], reverse=True)
        }
        cumsum = sum([v for k, v in dic.items()])
        eliminated = 0
        result = ""
        for index, item in enumerate(s):
            if index == 2:
                break
            result += f"`{item}`: {(s[item] / cumsum):.1%} "
            eliminated += s[item]
        if eliminated < cumsum:
            result += f"[other]: {(cumsum - eliminated) / cumsum:.1%}"
        return result
