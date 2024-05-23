import json
import os
import boto3
import pandas as pd
from io import BytesIO
import numpy as np  
from io import StringIO
from datetime import datetime

   
  
if_yes_who = {
    "Employee": "Employee",
    "Customer": "Customer (UGC)",
    "GSMC - EMEA": "GSMC-EMEA",
    "Creator/influencer": "Creator/Influencer",
    "N/A": "N/A",
    "PR": "PR",
    "Untagged":"Global"
}


tag_tier1_event = {
    "Prime Day": "Prime Day",
    "PEAS": "PEAS",
    "Holiday - Christmas": "Holiday - Christmas",
    "Holiday - Black Friday/Cyber Monday": "Holiday - Black Friday/Cyber Monday",
    "Spring Sale": "Spring Sale",
    "N/A": "N/A",
    "Prime Big Deals Day": "Prime Big Deal Days",
    "Untagged":"Global"
}
 
tag_reputational_topic = {
    "Brand building (evergreen content not tied to news cycle or announcement)": "Brand Building",
    "Community (charity and outreach)": "Community (Charity & Outreach)",
    "Community (Charity & Outreach)": "Community (Charity & Outreach)",
    "Community engagement (charity, outreach)": "Community (Charity & Outreach)",
    "Customer Trust (reviews, counterfeit)": "Customer Trust (reviews, counterfeit)",
    "Data Privacy and Security": "Data Privacy and Security",
    "Diversity Equity and inclusion": "DEI",
    "Diversity, equity, & inclusion (DEI)": "DEI",
    "Economic Impact (tax, investment etc)": "Econ impact (tax, investment)",
    "Economic impact (tax, investment)": "Econ impact (tax, investment)",
    "Engagement to drive Brand Love": "Brand Building",
    "New Product Launches": "Products & Services",
    "Price, selection and/or convenience": "Products & Services",
    "Products & services (AWS, Devices, Entertainment, Retail)": "Products & Services",
    "Products and Services (Prime, Price, Selection, Convenience, Products)": "Products & Services",
    "Supporting small & medium biz (SMB)": "Supporting SMB",
    "Supporting Small Business": "Supporting SMB",
    "Sustainability": "Sustainability",
    "Sustainability (all sustainability)": "Sustainability",
    "Workplace": "Workplace",
    "Workplace (Amazon as a good employer)": "Workplace",
    "Untagged":"Global",
    "AWS": "AWS",
    "Brand Building (evergreen not tied to news cycle or announcement)": "Brand Building",
    "Financial and Advertising Comms" : "Financial and Advertising Comms",
    "Financial or advertising comms": "Financial and Advertising Comms" ,
    "Entertainment": "Entertainment"
    }
 
     

tag_content_category_type = {
    "Type 1: Straight news amplification (i.e. sharing a blog or media coverage)": "Type 1: News and Announcements",
    "Type 2: Original content to support company news (i.e. making an original video for a store or device launch)": "Type 2: PR Campaigns",
    "Type 3: Original evergreen (i.e. repurposing employee-generated content for our own post)": "Type 3: Evergreen Content (GSMC)",
     "Untagged":"Global"
}

tag_team = {
    "Amazon in the community": "Amazon in the Community (AITC)",
    "Consumer Team": "Consumer PR",
    "Corporate Team": "Corporate PR",
    "Other Business Team (e.g. Hardlines, F360)": "Other Business Team",
    "Other PR Team": "Other PR Team",
    "Sustainability PR": "Sustainability PR",
    "Workplace": "Workplace PR",
    "XCM": "XCM",
"Untagged":"Global"
}
europe_mapping=['AT','BE','DE','ES','EU','FR','IT','NL','PL','SE','TR','UK']
# Define S3 client
s3 = boto3.client('s3')
   
# Define source and destination buckets and prefixes
source_bucket = 'wikitablescrapexample'
source_prefix = 'amazon_sprinklr_pull/finalmaster/'
tagg_bucket = 'wikitablescrapexample'
tagg_file_location = 'amazon_sprinklr_pull/latest_tag_mapping/'
post_message_folder = 'amazon_sprinklr_pull/mappingandbenchmark/'

paid_tag_file = "Paid_Tags_15_TagPull.json"
organic_tag_file = "Organic_Tags_12_TagPull.json"
post_message_file = "PostMessage_tagging.xlsx"

tag_geography_json_file = 'amazon_sprinklr_pull/latest_tag_mapping/Target_Geography_17_TagPull.json'
linkedin_region_lookup_file = 'amazon_sprinklr_pull/mappingandbenchmark/linkedinregionlookup.csv'


def read_csv_from_s3(bucket, key, encoding='utf-8'):
    """
    Reads a CSV file from an S3 bucket into a pandas DataFrame, using the specified encoding.

    Parameters:
    - bucket: The name of the S3 bucket.
    - key: The key of the CSV file in the S3 bucket.
    - encoding: The encoding to use for reading the CSV file. Defaults to 'utf-8'.

    Returns:
    - A pandas DataFrame containing the CSV data.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(BytesIO(obj['Body'].read()), encoding=encoding)
    return df

def read_json_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    tag_data = [json.loads(line) for line in BytesIO(obj['Body'].read()).readlines()]
    df = pd.DataFrame(tag_data)
    return df
        
def process_paid_data(df_data, df_tag, tag_post_message):
    df_tag = df_tag.add_prefix('paid_suffix_')
         
    
    df_combined = df_data.merge(df_tag, left_on='AD_VARIANT_NAME', right_on='paid_suffix_AD_VARIANT_NAME', how='left')
    
    # Clean and format the 'Is it XGC?' and 'paid_suffix_GSMC__WHO_IS_THE_SOURCE_OF_THE_XGC___OUTBOUND_MESSAGE' columns
    df_combined['Is it XGC?'] = df_combined['paid_suffix_C_63DAC6AC6DF27A45C687B642'].str.strip()
    df_combined['paid_suffix_C_63DAC8FF6DF27A45C68AAF7C'] = df_combined['paid_suffix_C_63DAC8FF6DF27A45C68AAF7C'].str.strip()
    
    # Check for 'Untagged', empty strings, or NaN in 'Is it XGC?' and replace with 'Global'
    df_combined['Is it XGC?'] = np.where(
        df_combined['Is it XGC?'].isin(['Untagged', '']) | df_combined['Is it XGC?'].isnull(),
        'Global',
        df_combined['Is it XGC?']
    )
               
    # # Create a case-insensitive mapping dictionary
    case_insensitive_mapping = {key.lower(): value for key, value in if_yes_who.items()}
    
    # # Map the values in 'If yes who made it?' column using the case-insensitive mapping dictionary
    df_combined['If yes who made it?'] = df_combined['paid_suffix_C_63DAC8FF6DF27A45C68AAF7C'].str.lower().map(case_insensitive_mapping).fillna(df_combined['paid_suffix_C_63DAC8FF6DF27A45C68AAF7C'])
    
    # After mapping the values, recheck 'Is it XGC?' column and set 'If yes who made it?' to 'N/A' if 'Is it XGC?' is 'no'
    df_combined.loc[df_combined['Is it XGC?'].str.lower() == 'no', 'If yes who made it?'] = 'N/A'
    
   # Copy values from 'paid_suffix_GSMC__CONTENT_CATEGORY_TYPE__OUTBOUND_MESSAGE' to 'Content Category Type' and map using the tag_content_category_type dictionary
    # df_combined['Content Category Type'] = df_combined['paid_suffix_GSMC__CONTENT_CATEGORY_TYPE__OUTBOUND_MESSAGE'].str.strip()
    # content_category_mapping = {key.lower(): value for key, value in tag_content_category_type.items()}
    # df_combined['Content Category Type'] = df_combined['Content Category Type'].str.lower().map(content_category_mapping).fillna(df_combined['Content Category Type'])
    # df_combined.loc[df_combined['Content Category Type'].isin(['', np.nan]), 'Content Category Type'] = 'Global'
    df_combined['Content Category Type']= 'Global'
         
   
    # Copy values from 'paid_suffix_GSMC__REPUTATIONAL_TOPIC__PAID_INITIATIVE' to 'Reputational Topic' and map using the tag_reputational_topic dictionary
    df_combined['Reputational Topic'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__PAID_INITIATIVE'].str.strip()

    reputational_topic_mapping = {key.lower(): value for key, value in tag_reputational_topic.items()}
    df_combined['Reputational Topic'] = df_combined['Reputational Topic'].str.lower().map(reputational_topic_mapping).fillna(df_combined['Reputational Topic'])
    df_combined.loc[df_combined['Reputational Topic'].isin(['', np.nan]), 'Reputational Topic'] = 'Global'
     
    # df_combined['Reputational Topic']= ''
    
    # # Copy values from 'paid_suffix_EVENT_CAMPAIGN_AMAZON_MARKETING___OUTBOUND_MESSAGE' to 'Tier 1 Event?' and map using the tag_tier1_event dictionary
    df_combined['Tier 1 Event?'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE'].str.strip()
    tier1_event_mapping = {key.lower(): value for key, value in tag_tier1_event.items()}
    df_combined['Tier 1 Event?'] = df_combined['Tier 1 Event?'].str.lower().map(tier1_event_mapping).fillna(df_combined['Tier 1 Event?'])
    df_combined.loc[df_combined['Tier 1 Event?'].isin(['', np.nan]), 'Tier 1 Event?'] = 'Global'
    
    # df_combined['Tier 1 Event?'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE"'].str.strip()
    # tier1_event_mapping = {key.lower(): value for key, value in tag_tier1_event.items()}
    # df_combined['Tier 1 Event?'] = df_combined['Tier 1 Event?'].str.lower().map(tier1_event_mapping).fillna(df_combined['Tier 1 Event?'])
    # df_combined.loc[df_combined['Tier 1 Event?'].isin(['', np.nan]), 'Tier 1 Event?'] = 'Global'
         
         
    df_combined['Team'] = df_combined['paid_suffix_GSMC_APAC_PR_TEAM__OUTBOUND_MESSAGE'].str.strip()
    team_mapping = {key.lower(): value for key, value in tag_team.items()}
    df_combined['Team'] = df_combined['Team'].str.lower().map(team_mapping).fillna(df_combined['Team'])
    df_combined.loc[df_combined['Team'].isin(['', np.nan]), 'Team'] = 'Global'
    
    df_combined.loc[~df_combined['paid_suffix_GSMC_APAC_ADS__CAMPAIGN_NAME__PAID_INITIATIVE'].str.strip().isna(), 'Post Message'] = df_combined['paid_suffix_GSMC_APAC_ADS__CAMPAIGN_NAME__PAID_INITIATIVE'].str.strip()

    post_message_mapping = {key.lower().strip(): value for key, value in tag_post_message.items()}
    df_combined['Post Message'] = df_combined['Post Message'].str.lower().str.strip().map(post_message_mapping).fillna(df_combined['Post Message'].str.strip())

    df_combined.loc[df_combined['Post Message'].isin([np.nan, '']), 'Post Message'] = 'Global'
      
    df_combined.loc[df_combined['Post Format'].isin([np.nan, '']), 'Post Format'] = 'Dark'
    
    df_combined['Audience'] = df_combined['paid_suffix_GCCI_SOME__ADS__AUDIENCE__AD_SET'].str.strip()
    df_combined.loc[df_combined['Audience'].isin(['', np.nan,"Untagged"]), 'Audience'] = 'Global'
 
    df_combined['Brand Lift Study'] = df_combined['paid_suffix_GCCI_SOME__ADS__BRAND_LIFT_STUDY__AD_SET'].str.strip()
    df_combined.loc[df_combined['Brand Lift Study'].isin(['', np.nan,"Untagged"]), 'Brand Lift Study'] = 'Global'
    
    df_combined['Campaign Description'] = df_combined['paid_suffix_GCCI_SOME__ADS__CAMPAIGN_DESCRIPTION__AD_SET'].str.strip()
    df_combined.loc[df_combined['Campaign Description'].isin(['', np.nan,"Untagged"]), 'Campaign Description'] = 'Global'
  
    df_combined['Requesting PR Org'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE'].str.strip()
    df_combined.loc[df_combined['Requesting PR Org'].isin(['', np.nan,"Untagged"]), 'Requesting PR Org'] = 'Global'
    
    # df_combined['Content Category Type - Intent'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__PAID_INITIATIVE'].str.strip()
    # df_combined.loc[df_combined['Content Category Type - Intent'].isin(['', np.nan,"Untagged"]), 'Content Category Type - Intent'] = 'Global'
    df_combined['Content Category Type - Intent'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__PAID_INITIATIVE'].str.strip()
    df_combined.loc[df_combined['Content Category Type - Intent'].isin(['', np.nan,"Untagged"]), 'Content Category Type - Intent'] = 'Global'
      
        
    df_combined['Breaking News?'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA__IS_THIS_POST_CONSIDERED_BREAKING_NEWS___OUTBOUND_MESSAGE'].str.strip()
    df_combined.loc[df_combined['Breaking News?'].isin(['', np.nan,"Untagged"]), 'Breaking News?'] = 'Global'
    
    df_combined['Content Source'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA__CONTENT_SOURCE__OUTBOUND_MESSAGE'].str.strip()
    df_combined.loc[df_combined['Content Source'].isin(['', np.nan,"Untagged"]), 'Content Source'] = 'Global'
    
     
    df_combined['If this is an XGC post, what kind is it?'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA__IF_THIS_IS_AN_XGC_POST__WHAT_KIND_IS_IT___OUTBOUND_MESSAGE'].str.strip()
    df_combined.loc[df_combined['If this is an XGC post, what kind is it?'].isin(['', np.nan,"Untagged"]), 'If this is an XGC post, what kind is it?'] = 'Global'
    
    
        
    df_combined['If Video or Reel how long?'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA__IF_VIDEO_REEL__HOW_LONG_IS_IT___OUTBOUND_MESSAGE'].str.strip()
    df_combined.loc[df_combined['If Video or Reel how long?'].isin(['', np.nan,"Untagged"]), 'If Video or Reel how long?'] = 'Global'
    
     
           
    df_combined['NA: If Workplace what was it about?'] = df_combined['paid_suffix_GCCI_SOCIAL_MEDIA_NA__IF__WORKPLACE___WHAT_WAS_IT_ABOUT___OUTBOUND_MESSAGE'].str.strip()
    df_combined.loc[df_combined['NA: If Workplace what was it about?'].isin(['', np.nan,"Untagged"]), 'NA: If Workplace what was it about?'] = 'Global'
    
    df_combined['SubCampaign'] = df_combined['paid_suffix_PAID_INITIATIVE_SPRINKLR_SUB-CAMPAIGN'].str.strip()
    df_combined.loc[df_combined['SubCampaign'].isin(['', np.nan,"Untagged"]), 'SubCampaign'] = 'Global'
    
    df_combined.loc[df_combined['Is it XGC?'] == 'Yes', 'Content Source'] = 'XGC'
     
     
    columns_to_drop = df_tag.columns
    # Drop the specified columns
    df_combined = df_combined.drop(columns=columns_to_drop, errors='ignore')
 
    
     
    return df_combined
    
  

def process_organic_tagging(df_data, df_tag, tag_post_message):
    
    df_tag.drop_duplicates(subset='PERMALINK', inplace=True)
        # Add prefix to the columns of df_tag
    df_tag = df_tag.add_prefix('organic_')
 
    #Merge df_data and df_tag on 'ACCOUNT_TYPE'
    df_combined = df_data.merge(df_tag, left_on='PERMALINK', right_on='organic_PERMALINK', how='left')
  
        #  Assign values from 'organic_62696510869F0F319C9BB681' to 'Tier 1 Event?' where data is present
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE'].isna(), 'Tier 1 Event?'] = df_combined['organic_GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE']
    
    # Map the values in 'Tier 1 Event?' using the case-insensitive mapping dictionary and keep original values where no match is found
    tier1_event_mapping = {key.lower(): value for key, value in tag_tier1_event.items()}
    df_combined['Tier 1 Event?'] = df_combined['Tier 1 Event?'].str.lower().map(tier1_event_mapping).fillna(df_combined['Tier 1 Event?'])
    
    # Replace any NaN or empty string values in 'Tier 1 Event?' with 'Global'
    df_combined.loc[df_combined['Tier 1 Event?'].isin([np.nan, '']), 'Tier 1 Event?'] = 'Global'
    
    
      # Assign values from 'organic_GSMC__IS_IT_XGC___OUTBOUND_MESSAGE' to 'Is it XGC?' where data is present
    df_combined.loc[~df_combined['organic_C_63DAC6AC6DF27A45C687B642'].isna(), 'Is it XGC?'] = df_combined['organic_C_63DAC6AC6DF27A45C687B642']
    
    # Replace any NaN or empty string values in 'Is it XGC?' with 'Global'
    df_combined.loc[df_combined['Is it XGC?'].isin([np.nan, '']), 'Is it XGC?'] = 'Global'
    
 
     # Update 'If yes who made it?' column
    df_combined.loc[~df_combined['organic_C_63DAC8FF6DF27A45C68AAF7C'].isna(), 'If yes who made it?'] = df_combined['organic_C_63DAC8FF6DF27A45C68AAF7C']
    if_yes_who_mapping = {key.lower(): value for key, value in if_yes_who.items()}
    df_combined['If yes who made it?'] = df_combined['If yes who made it?'].str.lower().map(if_yes_who_mapping).fillna(df_combined['If yes who made it?'])
    df_combined.loc[df_combined['If yes who made it?'].isin([np.nan, '']), 'If yes who made it?'] = 'Global'
    df_combined.loc[df_combined['Is it XGC?'].str.lower() == 'no', 'If yes who made it?'] = 'N/A'

   #
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__OUTBOUND_MESSAGE'].isna(), 'Reputational Topic'] = df_combined['organic_GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__OUTBOUND_MESSAGE']
    reputational_topic_mapping = {key.lower(): value for key, value in tag_reputational_topic.items()}
    df_combined['Reputational Topic'] = df_combined['Reputational Topic'].str.lower().map(reputational_topic_mapping).fillna(df_combined['Reputational Topic'])
    df_combined.loc[df_combined['Reputational Topic'].isin([np.nan, '']), 'Reputational Topic'] = 'Global'
 
  
  
  
    # df_combined.loc[~df_combined['organic_GSMC__CONTENT_CATEGORY_TYPE__OUTBOUND_MESSAGE'].isna(), 'Content Category Type'] = df_combined['organic_GSMC__CONTENT_CATEGORY_TYPE__OUTBOUND_MESSAGE']
    # content_category_mapping = {key.lower(): value for key, value in tag_content_category_type.items()}
    # df_combined['Content Category Type'] = df_combined['Content Category Type'].str.lower().map(content_category_mapping).fillna(df_combined['Content Category Type'])
    # df_combined.loc[df_combined['Content Category Type'].isin([np.nan, '']), 'Content Category Type'] = 'Global'
    
    df_combined['Content Category Type']= 'Global'
    
      
    
    
    df_combined.loc[~df_combined['organic_62696511869F0F319C9BB709'].isna(), 'Team'] = df_combined['organic_62696511869F0F319C9BB709']
    team_mapping = {key.lower(): value for key, value in tag_team.items()}
    df_combined['Team'] = df_combined['Team'].str.lower().map(team_mapping).fillna(df_combined['Team'])
    df_combined.loc[df_combined['Team'].isin([np.nan, '']), 'Team'] = 'Global'


    df_combined.loc[~df_combined['organic_CAMPAIGN'].str.strip().isna(), 'Post Message'] = df_combined['organic_CAMPAIGN'].str.strip()
    
    post_message_mapping = {key.lower().strip(): value for key, value in tag_post_message.items()}
    df_combined['Post Message'] = df_combined['Post Message'].str.lower().str.strip().map(post_message_mapping).fillna(df_combined['Post Message'].str.strip())
    
    df_combined.loc[df_combined['Post Message'].isin([np.nan, '']), 'Post Message'] = 'Global'
    
    
    df_combined['Media URL']=df_combined['organic_MEDIA_SOURCE']
        
        # For rows where 'Platform' is 'instagram Story', set 'Post Format' to 'instagram Story'
    df_combined.loc[df_combined['Platform'].str.lower() == 'instagram story', 'Post Format'] = 'Instagram Story'
    
    # For rows where 'Platform' is 'YouTube', set 'Post Format' to 'video'
    df_combined.loc[df_combined['Platform'].str.lower() == 'youtube', 'Post Format'] = 'Video'
    
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__OUTBOUND_MESSAGE'].isna(), 'Content Category Type - Intent'] = df_combined['organic_GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__OUTBOUND_MESSAGE']

    df_combined.loc[df_combined['Content Category Type - Intent'].isin([np.nan,'','Untagged']), 'Content Category Type - Intent'] = 'Global'

        
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA__IS_THIS_POST_CONSIDERED_BREAKING_NEWS___OUTBOUND_MESSAGE'].isna(), 'Breaking News?'] = df_combined['organic_GCCI_SOCIAL_MEDIA__IS_THIS_POST_CONSIDERED_BREAKING_NEWS___OUTBOUND_MESSAGE']

    df_combined.loc[df_combined['Breaking News?'].isin([np.nan,'','Untagged']), 'Breaking News?'] = 'Global'
    
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA__CONTENT_SOURCE__OUTBOUND_MESSAGE'].isna(), 'Content Source'] = df_combined['organic_GCCI_SOCIAL_MEDIA__CONTENT_SOURCE__OUTBOUND_MESSAGE']

    df_combined.loc[df_combined['Content Source'].isin([np.nan,'','Untagged']), 'Content Source'] = 'Global'
    
    #GCCI_SOCIAL_MEDIA__IF_THIS_IS_AN_XGC_POST__WHAT_KIND_IS_IT___OUTBOUND_MESSAGE
    
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA__IF_THIS_IS_AN_XGC_POST__WHAT_KIND_IS_IT___OUTBOUND_MESSAGE'].isna(), 'If this is an XGC post, what kind is it?'] = df_combined['organic_GCCI_SOCIAL_MEDIA__IF_THIS_IS_AN_XGC_POST__WHAT_KIND_IS_IT___OUTBOUND_MESSAGE']

    df_combined.loc[df_combined['If this is an XGC post, what kind is it?'].isin([np.nan,'','Untagged']), 'If this is an XGC post, what kind is it?'] = 'Global'
      
         
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA_NA__IF__WORKPLACE___WHAT_WAS_IT_ABOUT___OUTBOUND_MESSAGE'].isna(), 'NA: If Workplace what was it about?'] = df_combined['organic_GCCI_SOCIAL_MEDIA_NA__IF__WORKPLACE___WHAT_WAS_IT_ABOUT___OUTBOUND_MESSAGE']

    df_combined.loc[df_combined['NA: If Workplace what was it about?'].isin([np.nan,'','Untagged']), 'NA: If Workplace what was it about?'] = 'Global'
    # "GCCI_SOCIAL_MEDIA__IF_VIDEO_REEL__HOW_LONG_IS_IT___OUTBOUND_MESSAGE"
    
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA__IF_VIDEO_REEL__HOW_LONG_IS_IT___OUTBOUND_MESSAGE'].isna(), 'If Video or Reel how long?'] = df_combined['organic_GCCI_SOCIAL_MEDIA__IF_VIDEO_REEL__HOW_LONG_IS_IT___OUTBOUND_MESSAGE']

    df_combined.loc[df_combined['If Video or Reel how long?'].isin([np.nan,'','Untagged']), 'If Video or Reel how long?'] = 'Global'
     
    df_combined.loc[~df_combined['organic_GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE'].isna(), 'Requesting PR Org'] = df_combined['organic_GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE']

    df_combined.loc[df_combined['Requesting PR Org'].isin([np.nan,'','Untagged']), 'Requesting PR Org'] = 'Global'
    
    df_combined.loc[~df_combined['organic_SUB-CAMPAIGN'].isna(), 'SubCampaign'] = df_combined['organic_SUB-CAMPAIGN']

    df_combined.loc[df_combined['SubCampaign'].isin([np.nan,'','Untagged']), 'SubCampaign'] = 'Global'
    
    df_combined.loc[df_combined['Is it XGC?'] == 'Yes', 'Content Source'] = 'XGC'
    
      

    columns_to_drop = df_tag.columns
    # Drop the specified columns
    df_combined = df_combined.drop(columns=columns_to_drop, errors='ignore')


  
    return df_combined
    
 
#Post Format

def save_df_to_s3(df, bucket, key):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
       
def update_country(row):
    if row['Delivery'] == 'Paid' and row['Region'] == 'Europe':
        country_code = row['AD_VARIANT_NAME'][:2]  # Get first two letters
        if country_code in europe_mapping:
            return country_code
    return row['Country']

def get_files_to_concat(bucket):
    """
    Return list of filenames from `processedfiles.csv` where `need_to_add` is 0
    and also update the `need_to_add` to 1 for those files.
    """
    file_key = 'amazon_sprinklr_pull/tagged_clean_china_data/log_file/processedfiles.csv'
    obj = s3.get_object(Bucket=bucket, Key=file_key)
    csv_content = obj['Body'].read().decode('utf-8')
    df_log = pd.read_csv(StringIO(csv_content))

    # Filter out filenames where `need_to_add` is 0
    files_to_add = df_log[df_log['need_to_add'] == 0]['filename'].tolist()

    # Update the `need_to_add` value to 1 for these files
    df_log.loc[df_log['filename'].isin(files_to_add), 'need_to_add'] = 1

    # Save the updated dataframe back to S3
    csv_buffer = StringIO()
    df_log.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=file_key, Body=csv_buffer.getvalue())

    return files_to_add
    
def handle_geographies(df_tag_geography, df_linkedin_region):

    df_tag_geography = df_tag_geography[['PERMALINK', 'TARGETED_GEOGRAPHY']]
   
      

    df = df_tag_geography.merge(df_linkedin_region, how='left', left_on='TARGETED_GEOGRAPHY', right_on='TARGETED_GEOGRAPHY')
    

    # Handle missing Geographies
    df['Country'] = df['Country'].fillna('Global')
    df['Region'] = df['Region'].fillna('Global')

    return df

# def populate_countries_and_regions(df):
#     df_out = pd.DataFrame()
#     df_out['PERMALINK'] = df['PERMALINK'].unique()

#     countries = []
#     regions = []

#     for x in range(0, len(df_out)):
#         url = df_out['PERMALINK'][x]

#         # Get unique countries and regions for the permalink
#         unique_countries = df[df['PERMALINK'] == url]['Country'].unique()
#         unique_regions = df[df['PERMALINK'] == url]['Region'].unique()

#         # Check for unique country
#         if len(unique_countries) == 1:
#             countries.append(unique_countries[0])
#             regions.append(unique_regions[0] if len(unique_regions) == 1 else 'Multi Region')
#         # Check for unique region when country is not unique
#         elif len(unique_regions) == 1:
#             countries.append(unique_regions[0])
#             regions.append(unique_regions[0])
#         # Handling multiple or no unique countries and regions
#         else:
#             countries.append('Global')
#             regions.append('Multi Region' if len(unique_regions) > 1 else 'Global')

#     df_out['Country'] = countries
#     df_out['Region'] = regions

#     return df_out
        
        
def populate_countries_and_regions(df):
    df_out = pd.DataFrame()
    df_out['PERMALINK'] = df['PERMALINK'].unique()

    countries = []
    regions = []

    for x in range(0, len(df_out)):
        url = df_out['PERMALINK'][x]

        # Get unique countries and regions for the permalink
        unique_countries = df[df['PERMALINK'] == url]['Country'].unique()
        unique_regions = df[df['PERMALINK'] == url]['Region'].unique()

        # Check for unique country
        if len(unique_countries) == 1:
            countries.append(unique_countries[0])
            regions.append(unique_regions[0] if len(unique_regions) == 1 else 'Global')
        # Check for unique region when country is not unique
        elif len(unique_regions) == 1:
            countries.append(unique_regions[0])
            regions.append(unique_regions[0])
        # Handling multiple or no unique countries and regions
        else:
            if len(unique_regions) > 1:
                # Multiple unique regions
                countries.append('Multi-Region (LI only)')
                regions.append('Multi-Region (LI only)')
            else:
                # No unique region
                countries.append('Global')
                regions.append('Global')

    df_out['Country'] = countries
    df_out['Region'] = regions

    return df_out
             
                                                                                      
def lambda_handler(event, context):
    try:
        #csv_key = 'amazon_sprinklr_pull/for_tagging/final_need_to_tagg_2024-03-29_2024-05-13'
        csv_key=event['Records'][0]['s3']['object']['key']
        # Read CSV data from S3
        df_csv = read_csv_from_s3(source_bucket, csv_key,encoding='utf-8')
         
        # Filter df_csv for paid and organic data
        df_paid = df_csv[df_csv['is_paiddata'] == 1]
        df_organic = df_csv[df_csv['is_paiddata'] == 0]

        # Read PostMessage_tagging.xlsx file from S3
        post_message_key = os.path.join(post_message_folder, post_message_file)
        obj = s3.get_object(Bucket=source_bucket, Key=post_message_key)
        df_post_message = pd.read_excel(BytesIO(obj['Body'].read()), engine='openpyxl')

        # Convert df_post_message to a dictionary for mapping
        post_message_dict = df_post_message.set_index('Post Message')['cleaned_Post Message'].to_dict()

        # Read Organic JSON tag file from S3
        organic_json_key = os.path.join(tagg_file_location, organic_tag_file)
        df_organic_tag = read_json_from_s3(tagg_bucket, organic_json_key)

        # Get list of files to concatenate with df_organic
 
        file_path = 'amazon_sprinklr_pull/core_manual_tracker/processed_data.csv'
        df_to_add = read_csv_from_s3(source_bucket, file_path,encoding='utf-8')
       
        df_organic = pd.concat([df_organic, df_to_add], ignore_index=True)
  
        # Process the combined df_organic data
        df_organic_combined = process_organic_tagging(df_organic, df_organic_tag, post_message_dict)
        
        # Read and process Paid JSON tag file from S3
        paid_json_key = os.path.join(tagg_file_location, paid_tag_file)
        df_paid_tag = read_json_from_s3(tagg_bucket, paid_json_key)
        df_paid_tag.drop_duplicates(subset='AD_VARIANT_NAME', inplace = True)
        df_paid_combined = process_paid_data(df_paid, df_paid_tag, post_message_dict)
        
        # Concatenate the two processed DataFrames
        df_final_combined = pd.concat([df_organic_combined, df_paid_combined], ignore_index=True)

        # Iterate over rows where 'is_paiddata' is 0
        for index, row in df_final_combined[df_final_combined['is_paiddata'] == 0].iterrows():
            PERMALINK = row['PERMALINK']
            
            # Find the corresponding row where 'is_paiddata' is 1 and the 'PERMALINK' matches
            matching_row = df_final_combined[(df_final_combined['is_paiddata'] == 1) & (df_final_combined['PERMALINK'] == PERMALINK)]
            
            # Check if matching_row is not empty
            if not matching_row.empty:
                # Update columns in the row where 'is_paiddata' is 1 with values from the matching row where 'is_paiddata' is 0
                columns_to_update = ['Is it XGC?', 'Tier 1 Event?', 'If yes who made it?', 'Reputational Topic', 'Content Category Type', 'Team', 'Post Message', 'Post Format', 'Country', 'Region']
                for column in columns_to_update:
                    df_final_combined.at[matching_row.index[0], column] = row[column]
                    
                # Read "linkedinregionlookup.csv" into a DataFrame
        df_linkedin_region = read_csv_from_s3(source_bucket, linkedin_region_lookup_file,encoding='ISO-8859-1')
                 

        # Read "Target_Geography_17_TagPull.json" into a DataFrame
        df_tag_geography = read_json_from_s3(source_bucket, tag_geography_json_file)
        
        # Call the handle_geographies function
        df = handle_geographies(df_tag_geography, df_linkedin_region)
        
        # Call the populate_countries_and_regions function
        df_out = populate_countries_and_regions(df)
      
         

 

            
      
        
        # Initialize the 'is_targeted' column with blank strings
        df_final_combined['is_targeted'] = ""
        
        
        for index, row in df_out.iterrows():
            PERMALINK = row['PERMALINK']
            country = row['Country']
            region = row['Region']
            
            # Find the rows in df_final_combined that match the current PERMALINK
            matching_row_indices = df_final_combined[df_final_combined['PERMALINK'] == PERMALINK].index.tolist()
            
            # Loop through all matching indices and update them
            for idx in matching_row_indices:
                # Update 'Country' and 'Region' columns in df_final_combined
                df_final_combined.at[idx, 'Country'] = country
                df_final_combined.at[idx, 'Region'] = region
                
                # Check if the Platform is "LinkedIn" and update 'is_targeted'
                if df_final_combined.at[idx, 'Platform'] == "LinkedIn":
                    df_final_combined.at[idx, 'is_targeted'] = "Targeted"
               
        # For rows where 'PERMALINK' did not match and Platform is "LinkedIn", mark as "Not Targeted"
        mask = (df_final_combined['is_targeted'] == "") & (df_final_combined['Platform'] == "LinkedIn")
         
        df_final_combined.loc[mask, 'is_targeted'] = "Not Targeted"
        
      
        # Update 'Country' to 'EU' in df_final_combined where 'Country' is 'Europe' and 'Platform' is 'LinkedIn'
        df_final_combined.loc[(df_final_combined['Country'] == 'Europe') & (df_final_combined['Platform'] == 'LinkedIn'), 'Country'] = 'EU'
       
        df_final_combined['Country'] = df_final_combined.apply(update_country, axis=1)

             # Calculate the date range and save to S3
        date_interval = f"{df_final_combined['Pull Date'].min()}_{df_final_combined['Pull Date'].max()}"
        
        df_final_combined.loc[df_final_combined['is_manual_tracker'] != 1, 'is_manual_tracker'] = 0
        
            
        countries_to_exclude = ['BE', 'NL', 'PL', 'SE', 'TR', 'SG', 'CN']
 
# Filter the DataFrame to exclude rows where 'Country' is in the specified list
        df_final_combined = df_final_combined[~df_final_combined['Country'].isin(countries_to_exclude)]
        
        # condition = df_final_combined['is_manual_tracker'] == 1

        # df_final_combined.loc[condition & (df_final_combined['Content Source'] != 'XGC'), 'If this is an XGC post, what kind is it?'] = ''
        
        condition = df_final_combined['Content Source'] != 'XGC'

        # Apply the condition to all rows in the DataFrame
        df_final_combined.loc[condition, 'If this is an XGC post, what kind is it?'] = ''

          
         
        destination_key = 'amazon_sprinklr_pull/finalmaster/cleaned_master_tab_' + date_interval + '.csv'
        #destination_key='cleaned_master_tab_' + date_interval + '.csv'
        save_df_to_s3(df_final_combined, source_bucket, destination_key)

        return {
            'statusCode': 200,
            'body': 'Success! Both Paid and Organic data have been processed and saved to S3.'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {e}'
        }
      
