from bs4 import BeautifulSoup
from simhash import Simhash
import re
import logging
from app.dictionary.membership_keywords import membership_keywords

logger = logging.getLogger("HtmlSublimator")


class HtmlSublimator:
    # Define the allowed tags and their corresponding hex values
    TAG_HEX_MAP = {
        "div": "0", "ul": "1", "input": "2", "h2": "3", "h3": "4", "section": "5",
        "h1": "6", "h4": "7", "nav": "8", "form": "9", "table": "a", "header": "b",
        "footer": "c", "article": "d", "aside": "e", "main": "f"
    }

    def __init__(self, html_content):
        self.html_content = html_content
        try:
            self.soup = BeautifulSoup(self.html_content, 'html.parser')
            logger.debug("Successfully parsed HTML content.")
        except Exception as e:
            logger.error(f"Error parsing HTML content: {e}")
            self.soup = None

    def extract_readable_text(self):
        """
        Extract readable text from HTML content with special tags for title, meta, and body.
        """
        if not self.soup:
            logger.error("Soup object is not initialized.")
            return None

        try:
            title = f"{self.soup.title.string.strip()}" if self.soup.title and self.soup.title.string else ''
            meta_tag = self.soup.find('meta', attrs={'name': 'description'})
            meta = meta_tag['content'].strip() if meta_tag and 'content' in meta_tag.attrs else ''
            text = ' '.join(self.soup.stripped_strings)
            body = text.strip() if text else ''
            return f"[TITLE] {title} [/TITLE] [META] {meta} [/META] [BODY] {body} [/BODY]"
        except Exception as e:
            logger.debug(f"Error extracting readable text: {e}")
            return None

    def extract_simhash(self, bits=64):
        """
        Extract and return the Simhash value of the HTML content.
        """
        if not self.html_content:
            logger.debug("HTML content is empty. Returning known Simhash for this case.")
            return 0

        try:
            tokens = self.html_content.split()  # Using basic split() for efficiency
            simhash = Simhash(tokens, f=bits)
            return simhash.value
        except Exception as e:
            logger.error(f"Error extracting Simhash: {e}")
            return None

    def get_tag_sequence(self):
        """
        Cleans the DOM by keeping only the allowed tags and preparing the tag sequence.
        Also, removes attributes and text content.
        """
        if not self.soup:
            logger.error("Soup object is not initialized.")
            return None

        valid_tags = set(self.TAG_HEX_MAP.keys())
        tags_to_keep = []

        try:
            # Single pass to collect valid tags and remove attributes/text
            for tag in self.soup.find_all(True):
                tag_name = tag.name.lower().strip()
                if tag_name in valid_tags:
                    tags_to_keep.append(tag_name)
                    tag.attrs = {}  # Remove all attributes
                # Extract text to avoid including it in later passes
                if tag.string:
                    tag.string.extract()

            # Replace the tags with corresponding hex values
            hex_sequence = ''.join(self.TAG_HEX_MAP.get(tag, '') for tag in tags_to_keep)
            return hex_sequence
        except Exception as e:
            logger.error(f"Error extracting tag sequence: {e}")
            return None

    def detect_membership(self):
        """
        Detects if a login or signup form, link, or related text is present in the HTML content.
        Returns True if any such element is detected, otherwise False.
        """
        if not self.soup:
            logger.error("Soup object is not initialized.")
            return False

        keyword_pattern = r'(?<!\w)(?:' + '|'.join(map(re.escape, membership_keywords)) + r')(?!\w)'

        try:
            # Search in the body tag and relevant elements in one pass
            elements_to_check = [self.soup.body] if self.soup.body else []
            elements_to_check += self.soup.find_all(['form', 'a', 'button'])

            for element in elements_to_check:
                element_text = element.get_text(separator=' ').lower() if element else ''
                if re.search(keyword_pattern, element_text):
                    return True
                if element.name == 'a':
                    href = element.get('href', '').lower()
                    if re.search(keyword_pattern, href):
                        return True
                if element.name == 'form':
                    if self._check_text_and_action(element_text, element, keyword_pattern):
                        return True

            return False
        except Exception as e:
            logger.debug(f"Error detecting membership keywords: {e}")
            return False

    def _check_text_and_action(self, element_text, element, keyword_pattern):
        """
        Helper method to check for keywords in form elements or their actions.
        Returns True if a keyword is found in the element's text or action attribute.
        """
        try:
            return bool(re.search(keyword_pattern, element_text) or
                        re.search(keyword_pattern, element.get('action', '').lower()))
        except Exception as e:
            logger.error(f"Error checking form action or text: {e}")
            return False
