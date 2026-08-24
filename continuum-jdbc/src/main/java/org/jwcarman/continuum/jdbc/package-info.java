/*
 * Copyright © 2026 James Carman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * PostgreSQL persistence over a plain {@code DataSource}. Deliberately Postgres-flavored — claiming
 * uses {@code FOR UPDATE SKIP LOCKED} — rather than lowest-common-denominator ANSI. The reference
 * DDL ships as the classpath resource {@code org/jwcarman/continuum/jdbc/continuum-postgresql.sql};
 * applications own schema management and Continuum never executes DDL.
 */
package org.jwcarman.continuum.jdbc;
