#!/usr/bin/env python
# coding: utf-8

# In[1]:

# In[2]:


import great_expectations as gx
import pandas as pd

context = gx.get_context()

data_source_name = "my_data_source"
data_asset_name = "my_dataframe_data_asset"
batch_definition_name = "my_batch_definition"

data_source = context.data_sources.add_pandas(data_source_name)
data_asset = data_source.add_dataframe_asset(data_asset_name)
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

batch_definition = (
    context.data_sources.get(data_source_name)
        .get_asset(data_asset_name)
        .get_batch_definition(batch_definition_name))

df = pd.read_csv("SP_with_errors.csv")
batch_parameters = {"dataframe": df}

batch = batch_definition.get_batch(batch_parameters=batch_parameters)


# In[3]:


suite_name = "my_expectation_suite"
suite = gx.ExpectationSuite(name=suite_name)
suite = context.suites.add(suite)

for score in ['writing','math','reading']:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
        column=score+" score", max_value=100, min_value=0))

suite.add_expectation(
    gx.expectations.ExpectColumnDistinctValuesToEqualSet(
    column='gender',value_set=['male','female']))
suite.add_expectation(
    gx.expectations.ExpectColumnDistinctValuesToEqualSet(
    column='race/ethnicity',value_set=['group A','group B','group C','group D','group E']))
suite.add_expectation(
    gx.expectations.ExpectColumnDistinctValuesToEqualSet(
    column='lunch',value_set=['standard','free/reduced']))
suite.add_expectation(
    gx.expectations.ExpectColumnDistinctValuesToEqualSet(
    column='test preparation course',value_set=['none','completed']))

for column in df.columns[1:]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
        column=column))


# In[4]:


definition_name = "my_validation_definition"
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name=definition_name
)
validation_definition = context.validation_definitions.add(validation_definition)


# In[5]:


validation_results = validation_definition.run(batch_parameters = batch_parameters).to_json_dict()


# In[6]:


Expectations = {"expect_column_values_to_be_between":"Value between",
               "expect_column_values_to_not_be_null":"Value not null",
               "expect_column_distinct_values_to_equal_set":"Value in a set"}

Result_message = f"""```Data quality check success: {validation_results['success']}"""

for result in validation_results['results']:
    if result['success']==True:
            pass
    elif Expectations[result['expectation_config']['type']]!='Value in a set':
        Result_message += (f"""
        
    Test: {Expectations[result['expectation_config']['type']]}
    Column: {result['expectation_config']['kwargs']['column']}
    Success: {result['success']}
    Percentage of errors: {round(result['result']['unexpected_percent'],2)}%
    Count of errors: {result['result']['unexpected_count']}
    Error examples: {result['result']['partial_unexpected_list'][:5]}""")
        
    else:
        Unexpected_Items = list(set(result['result']['observed_value']) - set(result['expectation_config']['kwargs']['value_set']))
        
        Result_message += (f"""
        
    Test: {Expectations[result['expectation_config']['type']]}
    Column: {result['expectation_config']['kwargs']['column']}
    Success: {result['success']}
    Unexpected items: {Unexpected_Items}""")
        
Result_message += '```'


# In[7]:


import discord
from discord.ext import commands
import nest_asyncio
import asyncio
import os

nest_asyncio.apply()

async def Send():
    intents = discord.Intents.default()
    intents.guilds = True
    bot = commands.Bot(command_prefix="!", intents=intents)

    @bot.event
    async def on_ready():       
        for guild in bot.guilds:
            channel = discord.utils.get(guild.text_channels, name="main")
            if channel:
                await channel.send(Result_message)
        await bot.close()

    try:
        with open('Token.txt','r') as token_file:
            await bot.start(token_file.readline())
    except Exception as e:
        print(f"An error occurred: {e}")

asyncio.run(Send())


# In[ ]:




