# ------------------------------------------------------------------------------
#      Copyright 2024. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#
# ------------------------------------------------------------------------------
import binascii
from base64 import b64decode
from typing import Union, List

from thingsboard_gateway.gateway.entities.converted_data import ConvertedData


class EventPack:
    def __init__(self):
        self._data: List[ConvertedData] = []

    def append(self, data: Union[ConvertedData, List[ConvertedData], str, bytes]):
        if isinstance(data, list):
            self._data.extend(data)
        elif isinstance(data, (str, bytes)):
            str_data = data
            try:
                str_data = b64decode(data, validate=True)
            except binascii.Error:
                pass
            self._data.append(ConvertedData.deserialize(str_data))
        else:
            self._data.append(data)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)
