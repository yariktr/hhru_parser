from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

@dataclass(slots=True)
class Vacancy:
    id: str
    url: str
    source: str = "http"

    title: Optional[str] = None
    company_name: Optional[str] = None
    company_url: Optional[str] = None

    salary_from: Optional[int] = None
    salary_to: Optional[int] = None
    salary_currency: Optional[str] = None  
    is_gross: Optional[bool] = None
    salary_text: Optional[str] = None

    experience_text: Optional[str] = None  
    exp_bucket: Optional[str] = None       

    schedule: Optional[str] = None        
    employment_type: Optional[str] = None 
    location_city: Optional[str] = None

    responses_count: Optional[int] = None
    published_at: Optional[str] = None   
    description: Optional[str] = None
    skills: List[str] = field(default_factory=list)

    raw_json: Optional[Dict[str, Any]] = None
