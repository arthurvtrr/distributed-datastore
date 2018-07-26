/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.catalyst.serializer;

/**
 * The serializer type resolver is responsible for locating serializable types and their serializers.
 * <p>
 * Users can implement custom type resolvers to automatically register serializers. See {@link JdkTypeResolver}
 * for an example implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface SerializableTypeResolver {

  /**
   * Registers serializable types on the given {@link SerializerRegistry} instance.
   *
   * @param registry The serializer registry.
   */
  void resolve(SerializerRegistry registry);

}
