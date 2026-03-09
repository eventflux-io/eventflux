/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//
// Function Compatibility Tests
// Organized into logical modules for better maintainability.
//
// Reference: query/function/FunctionTestCase.java, CoalesceFunctionTestCase.java,
//            ConversionFunctionTestCase.java, UUIDFunctionTestCase.java

pub mod arithmetic;
pub mod case_expressions;
pub mod cast_functions;
pub mod combined;
pub mod math_functions;
pub mod string_functions;
pub mod utility_functions;
