"""
MIT License

Copyright (c) 2025 Mykhailo Koreshkov, Learn and Teach UA NGO

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import re
from collections import namedtuple
import pandas as pd

Lesson = namedtuple('Lesson', 'subject,grade,number')

def parse_lesson_id(lesson_id: str) -> Lesson:
    r = re.compile(r'([a-z]+)_C([1-9][0-9]*)L([1-9][0-9]*)')
    subj = r.search(lesson_id)[1]
    grade = r.search(lesson_id)[2]
    number = r.search(lesson_id)[3]
    if subj not in ['ukrlit','mova','history']:
        raise ValueError("Unknown subject (not in ['ukrlit','mova','history'])")
    return Lesson(subj,grade,number)

def create_copy_job(list_of_lessons: str):
    tokens = list_of_lessons.split(',')
    names = [ t.strip() for t in tokens ]
    lessons = [ parse_lesson_id(t) for t in names ]
    id = [  ]    

    