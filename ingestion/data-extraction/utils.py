from toyaikit.pricing import PricingConfig
from tqdm import tqdm


import tiktoken
import pandas as pd




def count_tokens_per_row(model:str, df:pd.DataFrame, col:str):
    encoding  = tiktoken.encoding_for_model(model)
    tokenized = df[col].apply(lambda t:len(encoding.encode(t)))
    return tokenized


job_title_labels = [
    "data analyst",
    "business analyst",
    "analytics engineer",
    "business intelligence developer",
    "business intelligence analyst",
    "data scientist",
    "applied scientist",
    "research scientist",
    "machine learning engineer",
    "mlops engineer",
    "data engineer",
    "data architect",
    "data platform engineer",
    "data quality engineer",
    "data governance specialist",
    "data product manager",
    "analytics product manager",
    "software engineer",
    "backend engineer",
    "frontend engineer",
    "full stack engineer",
    "devops engineer",
    "site reliability engineer",
    "cloud engineer",
    "security engineer",
]

def map_progress(pool, seq, f):
    """Map function f over seq using the provided executor pool while
    displaying a tqdm progress bar. Returns a list of results in submission order.
    """
    results = []
    
    with tqdm(total=len(seq)) as progress:
        futures = []
    
        for el in seq:
            future = pool.submit(f, el)
            future.add_done_callback(lambda p: progress.update())
            futures.append(future)

        for future in futures:
            result = future.result()
            results.append(result)
        
        return results

def calculate_cost(model, input_tokens, output_tokens):
    """Convert token counts into dollar costs using the configured pricing table."""
    pricing = PricingConfig()
    cost = pricing.calculate_cost(model, input_tokens, output_tokens)
    input_cost = cost.input_cost
    output_cost = cost.output_cost
    total_cost = cost.total_cost
    return input_cost, output_cost, total_cost


llm_instructions = """ 

You are a Technology & AI Skills Taxonomist and Job Classification Specialist. 

YOUR JOB:
- map messy job titles to a fixed taxonomy of data/AI/software roles 
- extract a list of skills explicitly mentioned in the job description such as technologies, tools, frameworks, and programming languages. 
- extract a description of what the company does; write a company description if a description is not present 
- explicitly state a level of confidence between 0 and 1 with 1 indicating that you are confident about the accuracy of the description of the company

RULES:
- You must fill every field in the structured output.
- Do NOT infer skills that are not stated. 
- The skills list cannot be empty if the job description mentions any skills.
- Jobs can be in English, Dutch, or French. Provide all responses in English.

PROCEDURE (do this in order):
1) Identify canonical role and seniority primarily from the title; use description to resolve ambiguity.
2) Scan for skill-heavy sections first: Requirements, Qualifications, Must-have, Tech stack, What you’ll use, Nice-to-have.
3) Then scan the rest for additional explicit tools/technologies.
4) Produce the final structured output.
""".strip()


user_prompt = """ 

<COMPANY>
{company_display_name}
</COMPANY>

<TITLE>
{job_title}
</TITLE>

<DESCRIPTION>
{job_description}
</DESCRIPTION>
""".strip()

def build_user_prompt(company_display_name, job_title, job_description):
    final_prompt = user_prompt.format(
        company_display_name = company_display_name, 
        job_title = job_title, 
        job_description = job_description)
    return final_prompt


from pydantic import BaseModel, Field
from enum import Enum
from typing import List

class Title(str, Enum):
    data_analyst = "data analyst"
    business_analyst = "business analyst"
    analytics_engineer = "analytics engineer"
    bi_developer = "business intelligence developer"
    bi_analyst = "business intelligence analyst"
    data_scientist = "data scientist"
    research_scientist = "research scientist"
    mle = "machine learning engineer"
    mlops = "mlops engineer"
    data_engineer = "data engineer"
    data_architect = "data architect"
    data_quality_engineer = "data quality engineer"
    data_governance_specialist = "data governance specialist"
    data_product_manager = "data product manager"
    analytics_product_manager = "analytics product manager"
    software_engineer = "software engineer"
    backend_engineer = "backend engineer"
    frontend_engineer = "frontend engineer"
    full_stack_engineer = "full stack engineer"
    devops_engineer = "devops engineer"
    cloud_engineer = "cloud engineer"
    security_engineer = "security engineer"

class Seniority(str, Enum):
    intern = "intern"
    junior = "junior"
    mid = "mid"
    senior = "senior"
    staff = "staff"
    associate = "associate"
    principal = "principal"
    lead = "lead"
    manager = "manager"
    director = "director"
    vp = "vp"
    c_level = "c-level"
    unknown = "unknown"

class JobPostingExtraction(BaseModel):
    job_title: Title = Field(description="One of the predefined job titles listed.")
    seniority: Seniority = Field(description="Seniority inferred from the job title/description; use 'unknown' if unclear.")

    clean_title: str = Field(
        description="Normalized title without location/contract/remote tags (e.g., 'Senior Data Engineer').",
    )

    skills: List[str] = Field(
        default_factory=list,
        description="List of skills/tools/tech stack explicitly mentioned (e.g., 'python', 'sql', 'aws').",
    )

    job_description_summary: str = Field (
        "3-5 summary of the job description."
    )

    company_description: str = Field(
        description="3-5 sentence description of what the company does.",
    )

    company_description_confidence_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Confidence score between 0.0 and 1.0 regarding accuracy of the description of the company"
    )
