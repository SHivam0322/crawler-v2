from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import time
import os

def set_options():
    options = Options()
    options.headless = True
    # options.headless = False
    return options

def driver(options=set_options()):
    driver_path = os.path.abspath(r"lib\chromedriver")
    # print("driver_path::", driver_path)
    driver = webdriver.Chrome(options=options, executable_path=driver_path)
    return driver

def scroll_to_bottom(driver):
    old_position = 0
    new_position = None
    while new_position != old_position:
        # Get old scroll position
        old_position = driver.execute_script(
                ("return (window.pageYOffset !== undefined) ?"
                 " window.pageYOffset : (document.documentElement ||"
                 " document.body.parentNode || document.body);"))
        # Sleep and Scroll
        time.sleep(3)
        driver.execute_script((
                "var scrollingElement = (document.scrollingElement ||"
                " document.body);scrollingElement.scrollTop ="
                " scrollingElement.scrollHeight;"))
        # Get new position
        new_position = driver.execute_script(
                ("return (window.pageYOffset !== undefined) ?"
                 " window.pageYOffset : (document.documentElement ||"
                 " document.body.parentNode || document.body);"))
