import asyncio
import os
from dataclasses import dataclass, field
from pprint import pformat
from typing import List, Dict, Tuple, Optional, Any
import openai
import requests
import time
from loguru import logger
from aiohttp import ClientResponseError
from openai.error import RateLimitError, ServiceUnavailableError


OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', 'your-api-key')


@dataclass
class GPTTagger:
    """
    A class for tagging company descriptions with verticals and corresponding industry groups using LLMs.

    NOTE: YOU CAN INSTANTIATE THIS CLASS WITH YOUR OWN MODEL PARAMETERS.
    NOTE: THIS CLASS CURRENTLY SUPPORTs BOTH GPT AND LLAMA COMPLETIONS. TO USE LLAMA, SET model_type TO "llama".
    NOTE: IN THE FUTURE, TO USE DIFFERENT VERSION OF Llama (e.g. Llama 2 13B), YOU CAN CHANGE THE llama_endpoint TO THE ENDPOINT OF YOUR CHOICE.

    SEE: https://huggingface.co/docs/api-inference/detailed_parameters for more information about Llama parameters.

    Attributes:
        model_type (str): The type of model to be used for chat completion. Accepted models include "gpt" and "'llama". Defaults to "gpt".
        llama_endpoint (str): The endpoint for the llama model. Defaults to endpoint to ARBM Huggingface instance of Llama 2 7B.
        API_KEY (str): The API key to your service.
        verticals (List[str]): The list of verticals.
        sectors (Dict): The dictionary mapping industry groups to their corresponding verticals.
        model (str): OpenAI Model to be used for chat completion. Defaults to "gpt-3.5-turbo-16k".
        temperature (float): The temperature parameter for text generation. Defaults to 0.
        max_tokens (int): The maximum number of tokens for text generation. Defaults to 50.
        top_p (float): The top-p parameter for text generation. Defaults to 0.
        frequency_penalty (float): The frequency penalty parameter for text generation. Defaults to 0.
        presence_penalty (float): The presence penalty parameter for text generation. Defaults to 0.
        role (str): The role of the assistant.
        industry_prompt (str): The prompt for classifying industries based on descriptions for GPT model.
        vertical_prompt (str): The prompt for classifying verticals based on descriptions for GPT model.
        industry_prompt_llama (str): The prompt for classifying industries based on descriptions for Llama model.
        vertical_prompt_llama (str): The prompt for classifying verticals based on descriptions for Llama model.

    Methods:
        __post_init__: Initializes the GPTTagger object after it has been created.
        chat_completion: Generates a chat completion based on the given prompt using the GPT model.
        llama_query: Queries the Llama model. Helper function for llama_completion.
        llama_completion: Generates a chat completion based on the given prompt using the Llama model.
        classify_industries: Classifies industries/verticals based on the given company description. Supports both GPT and Llama models.
        tag: Wrapper function for classify_industries. Tags the given company description by classifying its verticals and corresponding industry groups. Handles rate limit errors.
    """

    # Attributes
    model_type: str = field(default="gpt") # the type of model to be used for chat completion
    llama_endpoint: str = field(default="https://eau3r19hz6ohithu.us-east-1.aws.endpoints.huggingface.cloud") # the endpoint for the llama model
    API_KEY: str = OPENAI_API_KEY

    # Classification Parameters
    verticals: List[str] = field(default_factory=list)
    sectors: Dict = field(default_factory = lambda: {
    "INFORMATION TECHNOLOGY": ["Business Information Systems", "Cloud Data Services", "Cloud Management", "Cloud Security", "Cyber Security", "Data Center", "Data Integration", "Data", "Artificial Intelligence"],
    "HEALTH CARE": ["Alternative Medicine", "Biopharma", "Clinical Trials", "Genetics", "Health Care", "Health Diagnostics", "Hospital", "Medical", "Medical Device", "Wellness"],
    "FINANCIAL SERVICES": ["Accounting", "Banking", "Credit", "Finance", "Financial Services", "FinTech", "Insurance", "Lending", "Payments", "Cryptocurrency"],
    "CONSUMER ELECTRONICS": ["Computer", "Drones", "Electronics", "Mobile Devices", "Smart Home", "Wearables", "Gaming"],
    "COMMERCE AND SHOPPING": ["E-Commerce", "Retail", "Marketplace", "Local Shopping", "Personalization", "Point of Sale", "Wholesale"],
    "ENERGY": ["Battery", "Clean Energy", "Energy Management", "Energy Storage", "Renewable Energy", "Solar", "Wind Energy", "Fossil Fuels"],
    "REAL ESTATE": ["Architecture", "Commercial Real Estate", "Construction", "Property Development", "Property Management", "Rental Property", "Residential", "Smart Building"],
    "TRANSPORTATION": ["Air Transportation", "Automotive", "Autonomous Vehicles", "Car Sharing", "Delivery Service", "Electric Vehicle", "Fleet Management", "Logistics", "Public Transportation", "Ride Sharing", "Robotics"],
    "MOBILE AND TELECOMUNICATIONS": ["Android", "iOS", "Mobile", "Mobile Payments", "Wireless", "SMS", "Unified Communications", "VoIP"],
    "SOFTWARE SERVICES": ["Apps", "Cloud Computing", "CRM", "Database", "Developer Tools", "Enterprise Software", "Mobile Apps", "Software Engineering", "Web Apps"],
    "SUSTAINABILITY": ["CleanTech", "Recycling", "Waste Management", "Water Purification"]})

    # Model parameters
    gpt_model: str = field(default="gpt-3.5-turbo-16k")
    temperature: float = field(default=0.1)
    max_tokens: int = field(default=50)
    top_p: float = field(default=0.1)
    frequency_penalty: float = field(default=0)
    presence_penalty: float = field(default=0)
    role: str = field(default="You are an assistant that classifies industries based on descriptions")

    # Prompts for GPT model
    industry_prompt: str = field(default="Determine if the given company description aligns with the specified Industry Group: \"{group}\". Respond only with \"Yes\" or \"No\".\n\nDescription:\n{description}")
    vertical_prompt: str = field(default="Classify the given start-up description into the most relevant industry groups. Return a maximum of 4 industries, sorted by relevance. Use the format 'Industry1, Industry2, ...'.\n\nIndustry Groups:\n{groups}\n\nDescription:\n{description}")

    # Prompts for Llama model
    industry_prompt_llama: str = field(default="<s>[INST] <<SYS>> Your task is to determine if a given company description aligns with a specified Industry Group. Respond only with \"Yes\" or \"No\". <</SYS>> Determine if the given company description aligns with the specified Industry Group: \"{group}\". Description: {description} [/INST]</s> Be very strict with your responses. If you are unsure, respond with \"No\".")
    vertical_prompt_llama: str = field(default="<s>[INST] <<SYS>> Task: Classify the start-up description into industries. Only return a list of industries, each separated by commas. Do not include any numbers, explanatory text, or additional information. Available Industries: {groups}. Description: {description}. <</SYS>> [/INST]</s>")

    def __post_init__(self):
        """
        Initializes the GPTTagger object after it has been created.

        Raises:
            Exception: If the API key is not provided or is set to "your-api-key".
        """
        # Check if the API key is valid, if not then throw an error
        if self.API_KEY in [None, "", "your-api-key"]:
            raise ValueError("Valid API key is required for GPTTagger.")

        if self.model_type == "gpt":
            openai.api_key = self.API_KEY
        else:
             self.headers = {"Authorization": "Bearer {key}".format(key=self.API_KEY), "Content-Type": "application/json"}


    async def chat_completion(self, prompt: str) -> str:
            """
            Generates a GPT chat completion based on the given prompt.

            Args:
                prompt (str): The user's input prompt.

            Returns:
                str: The generated chat completion. An empty string is returned if an error occurs.

            Raises:
                RateLimitError: If the API request limit has been reached.
                ServiceUnavailableError: If the API is unavailable.
            """
            try:
                completion = await openai.ChatCompletion.acreate(
                    model=self.gpt_model,
                    messages=[
                        {"role": "system", "content": self.role}, # assume the role of the assistant to be constant
                        {"role": "user", "content": prompt}
                    ],
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    top_p=self.top_p,
                    frequency_penalty=self.frequency_penalty,
                    presence_penalty=self.presence_penalty
                )
                # logger.critical(pformat(completion))
                return completion.choices[0].message['content']
            except (RateLimitError, ServiceUnavailableError) as e:
                logger.error(f'Error encountered: {e}')
                return ""


    def llama_query(self, payload: dict, max_retries: int = 6, retry_interval: int = 300) -> list:
        """
        Sends a query to the Llama endpoint and returns the response as a JSON object.

        Args:
            payload (dict): The payload to be sent in the request.
            max_retries (int): Maximum number of retries for the request.
            retry_interval (int): Time to wait between retries in seconds. It usually takes 5 minutes for the server to scale back from zero.

        Returns:
            list: The response from the Llama endpoint as a JSON object. An empty list is returned if an error occurs.

        Raises:
            requests.exceptions.HTTPError: If an HTTP error other than 502 occurs when querying Llama.
            requests.exceptions.RequestException: If a request error occurs when querying Llama.
            Exception: If an unexpected error occurs when querying Llama.
        """
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = requests.post(self.llama_endpoint, headers=self.headers, json=payload, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 502 and retry_count < max_retries - 1: # wait and retry of the server is down
                    logger.warning(f"Server returned 502 error, retrying in {retry_interval} seconds (retry {retry_count+1}/{max_retries})...")
                    time.sleep(retry_interval)
                    retry_count += 1
                else:
                    logger.error(f'HTTP error occurred when querying Llama: {e}')
                    return []
            except requests.exceptions.RequestException as e:
                logger.error(f'Request error occurred when querying Llama: {e}')
                break
            except Exception as e:
                logger.error(f'Unexpected error occurred when querying Llama: {e}')
                break
        return []  # Return an empty list if an error occurs or all retries are exhausted


    async def llama_completion(self, prompt: str) -> str:
            """
            Completes a given prompt using the Llama model.

            Args:
                prompt (str): The prompt to be completed.

            Returns:
                str: The generated text as a result of the completion. An empty string is returned if an error occurs.
            """
            response = self.llama_query({"inputs": prompt,
                                         "parameters": {"max_new_tokens": self.max_tokens,
                                                        "temperature": self.temperature,
                                                        "top_p": self.top_p,
                                                        "return_full_text": False,
                                                         # the server scales to zero after 15 minutes of inactivity. This will cause the first request to wait until the server is ready instead of returning a 503 error.
                                                        },
                                         "options": {"wait_for_model": True}
                                        })

            if not response:   # responce is empty
                raise Exception("Error occurred when querying Llama")
            return response[0]["generated_text"]


    async def classify_industries(self, description: str, industries: Optional[List[str]] = None) -> Tuple[List[str], List[str]]:
        """
        Classifies industries/verticals based on the given company description.

        Will use the LLM specified in the model_type attribute.

        Args:
            description (str): Company description.
            industries (Optional[List[str]], optional): If we want only the subset of industries. SEE tag(). Defaults to None.

        Returns:
            Tuple[List[str], List[str]]: A tuple containing a list of verticals and a list of corresponding industries.
        """
        industries = []
        verticals_list = []
        all_verticals = [item for sublist in self.sectors.values() for item in sublist] # get a list of all valid verticals

        # choose the appropriate completion method based on the model type
        completion_methods = {
            "gpt": self.chat_completion,
            "llama": self.llama_completion
        }

        # Validate model type
        if self.model_type not in completion_methods:
            raise ValueError("Invalid model type specified. Choose 'gpt' or 'llama'.")

        if industries:
            sectors = industries
        else:
            sectors = self.sectors.keys()

        # Get the appropriate completion method
        completion_method = completion_methods[self.model_type]

        for industry in sectors:
            industry_prompt = self.industry_prompt if self.model_type == "gpt" else self.industry_prompt_llama

            logger.info(f'using industry prompt: {industry_prompt.format(group=industry, description=description)}')
            industry_answer = await completion_method(industry_prompt.format(group=industry, description=description))

            logger.info(f'\nresponse: {industry_answer}')

            # query for verticals only if the industry is valid
            if 'yes' in industry_answer.strip().lower(): # based on unconsistent responses from Llama, we check if the answer contains the word 'yes' instead of checking for equality
                industries.append(industry)
                vertical_prompt = self.vertical_prompt if self.model_type == "gpt" else self.vertical_prompt_llama
                vertical_answer = await completion_method(vertical_prompt.format(groups=self.sectors[industry], description=description))

                verticals_list.extend([vertical.strip() for vertical in vertical_answer.split(",") if vertical.strip() in all_verticals])

        return verticals_list, industries


    async def llama_true_false_classifier(self, params: Dict[str, Any] = None, prompt: str = None) -> bool:
            """
            Classifies the response generated by the llama_query method as True or False.

            Args:
                params (Dict[str, Any]): Optional parameters for the llama_query method.
                prompt (str): The input prompt for the llama_query method.

            Returns:
                bool: True if the response contains the word "true", False if it contains the word "false".

            Raises:
                ValueError: If invalid parameters or prompt are provided.
                RuntimeError: If no response is received from the llama_query method.
                ValueError: If the response cannot be confirmed as True or False.
            """

            if params is None or prompt is None:
                raise ValueError("Invalid parameters or prompt. Both parameters and prompt must be specified.")

            response = self.llama_query({"inputs": prompt, "parameters": params})

            if not response:
                logger.error("No response received.")
                raise RuntimeError("No response received.")

            response_text = response[0]["generated_text"].strip().lower()
            if "true" in response_text:
                return True
            elif "false" in response_text:
                return False
            else:
                logger.error(f"Unable to confirm response: {response[0]['generated_text']}")
                raise ValueError(f"Unable to confirm response: {response[0]['generated_text']}")


    async def tag(self, description: str, sleep_time: int = 5, industry_subset: List[str] = None) -> Tuple[List[str], List[str]]:
        """
        Wrapper function for classify_industries.
        Tags the given company description by classifying its verticals and corresponding industry groups.

        Args:
            description (str): Company description to be tagged.
            sleep_time (int, optional): The sleep time in seconds before returning the results. Defaults to 5.

        Returns:
            tuple: A tuple containing the list of verticals and corresponding industry groups. A tuple of empty lists is returned if an error occurs.
        """

        if industry_subset and not set(industry_subset).issubset(self.sectors.keys()):
            raise ValueError("Unsupported industries specified. Choose from the following: {}".format(self.sectors.keys()))

        try:
            verticals_list, industries = await self.classify_industries(description, industry_subset)
            await asyncio.sleep(sleep_time) # sleep for 5 seconds to avoid rate limit
            return verticals_list, industries
        except Exception as e:
            logger.error(f'Unexpected error: {e}')
            return [], []





# LEGACY CODE
# async def classify_industries(self, description: str) -> Tuple[List[str], List[str]]:
    #         """
    #         Classifies industries/verticals based on the given company description.

    #         Args:
    #             description (str): Company description.

    #         Returns:
    #             Tuple[List[str], List[str]]: A tuple containing a list of verticals and a list of corresponding industries.
    #         """
    #         industries = []
    #         verticals_list = []
    #         all_verticals = [item for sublist in self.sectors.values() for item in sublist]

    #         for industry in self.sectors.keys():
    #             industry_answer = await self.chat_completion(self.industry_prompt.format(group=industry, description=description))
    #             if industry_answer.strip().lower() == 'yes':
    #                 industries.append(industry)
    #                 vertical_answer = await self.chat_completion(self.vertical_prompt.format(groups=self.sectors[industry], description=description))
    #                 verticals_list.extend([vertical.strip() for vertical in vertical_answer.split(",") if vertical.strip() in all_verticals]) # we check if the vertical is in the list of all verticals to avoid adding duplicates or invalid verticals

    #         return verticals_list, industries

# async def tag(self, description: str, sleep_time: int = 5) -> Tuple[List[str], List[str]]:
    #         """
    #         Wrapper function for classify_industries.
    #         Tags the given company description by classifying its verticals and corresponding industry groups.

    #         Args:
    #             description (str): Company description to be tagged.
    #             sleep_time (int, optional): The sleep time in seconds before returning the results. Defaults to 5.

    #         Returns:
    #             tuple: A tuple containing the list of verticals and corresponding industry groups. A tuple of empty lists is returned if an error occurs.
    #         """
    #         try:
    #             verticals_list, industries = await self.classify_industries(description)
    #             await asyncio.sleep(sleep_time) # sleep for 5 seconds to avoid rate limit
    #             return verticals_list, industries
    #         except Exception as e:
    #             logger.error(f'Unexpected error: {e}')
    #             return [], []


    # def llama_query(self, payload: dict) -> list:
    #     """
    #     Sends a query to the Llama endpoint and returns the response as a JSON object.

    #     NOTE: THIS FUNCTION IS NOT ASYNCHRONOUS. IF YOU WANT TO USE THIS FUNCTION ASYNCHRONOUSLY, see llama_query_async INSTEAD. YOU'll HAVE TO MODIFY llama_completion TO USE llama_query_async.

    #     Args:
    #         payload (dict): The payload to be sent in the request. See headers in __post_init__ for more information.

    #     Returns:
    #         list: The response from the Llama endpoint as a JSON object. An empty list is returned if an error occurs.

    #     Raises:
    #         requests.exceptions.HTTPError: If an HTTP error occurs when querying Llama.
    #         requests.exceptions.RequestException: If a request error occurs when querying Llama.
    #         Exception: If an unexpected error occurs when querying Llama.

    #     """
    #     try:
    #         response = requests.post(self.llama_endpoint, headers=self.headers, json=payload, timeout=10)
    #         response.raise_for_status()
    #         return response.json()
    #     except requests.exceptions.HTTPError as e:
    #         logger.error(f'HTTP error occurred when querying Llama: {e}')
    #     except requests.exceptions.RequestException as e:
    #         logger.error(f'Request error occurred when querying Llama: {e}')
    #     except Exception as e:
    #         logger.error(f'Unexpected error occurred when querying Llama: {e}')
    #     return []  # Return an empty list if an error occurs
