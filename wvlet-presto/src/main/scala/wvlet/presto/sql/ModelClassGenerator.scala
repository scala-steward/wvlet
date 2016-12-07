/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.presto.sql

import java.lang.reflect._
import java.util.Optional

import com.facebook.presto.sql.tree.Node
import wvlet.log.LogSupport
import wvlet.log.io.Resource
import wvlet.obj.{GenericType, ObjectType}
import wvlet.presto.sql.ModelClassGenerator.{ModelParam, RootConstructor}

case class ModelClass(cl:Class[_]) extends LogSupport {
  import ModelClassGenerator._

  override def toString = s"${if(isAbstract) "abstract " else ""}${name}"
  def name = cl.getSimpleName
  def isAbstract = Modifier.isAbstract(cl.getModifiers)

  lazy val fields : Seq[wvlet.obj.ConstructorParameter] = {
    val params =
      for((f, i) <- cl.getDeclaredFields.zipWithIndex
          if Modifier.isFinal(f.getModifiers)) yield {
        wvlet.obj.ConstructorParameter(cl, None, i, f.getName, resolveTypeOf(f.getGenericType))
      }
    params.toSeq
  }

  lazy val getterParameters : Seq[ModelParam] = {
    val getterMethods =
      cl.getDeclaredMethods()
      .filter { m =>
        val name = m.getName
        name.startsWith("get") || name.startsWith("is")
      }

    getterMethods.map { m =>
      val name = m.getName.replaceFirst("^(is|get)", "")
      val paramName = s"${name.charAt(0).toLower}${name.substring(1)}"
      val returnType = resolveTypeOf(m.getGenericReturnType)
      ModelParam(paramName, returnType)
    }
  }

  def findRootConstructor: Option[RootConstructor] = {
    info(s"${name}\n - ${fields.mkString("\n - ")}")
    cl.getDeclaredConstructors
    .find(isRootModelClassConstructor)
    .map{ c =>
      val params = c.getParameters
      val modelParams = params.map { p =>
        val tpe = resolveParameter(p)
        ModelParam("", tpe)
      }
      RootConstructor(c, modelParams)
    }
  }

  private def isRootModelClassConstructor(c:Constructor[_]) = {
    val params = c.getParameters
    if (params.length <= 0) {
      false
    }
    else {
      // Check the presence of Optional<NodeLocation>
      isJavaOptional(params(0).getType) && {
        val p = params(0).getParameterizedType
        p.isInstanceOf[ParameterizedType] && {
          val pt = p.asInstanceOf[ParameterizedType]
          val args = pt.getActualTypeArguments
          args.length > 0 && args(0).getTypeName == "com.facebook.presto.sql.tree.NodeLocation"
        }
      }
    }
  }
}

/**
  *
  */
object ModelClassGenerator extends LogSupport {

  case class RootConstructor(constructor: Constructor[_], param:Seq[ModelParam])
  case class ModelParam(name:String, valueType:ObjectType)

  private[sql] def findNodeClasses : Seq[ModelClass] = {
    val packageName = "com.facebook.presto.sql.tree"
    val classFileList = Resource.listResources(packageName, _.endsWith(".class"))

    def componentName(path: String): Option[String] = {
      val dot: Int = path.lastIndexOf(".")
      if (dot <= 0) {
        None
      }
      else {
        Some(path.substring(0, dot).replaceAll("/", "."))
      }
    }
    def findClass(name: String): Option[Class[_]] = {
      try
        Some(Class.forName(name, false, Thread.currentThread().getContextClassLoader))
      catch {
        case e: ClassNotFoundException => None
      }
    }
    val b = Seq.newBuilder[ModelClass]
    for (vf <- classFileList; cn <- componentName(vf.logicalPath)) {
      val className: String = packageName + "." + cn
      for (cl <- findClass(className)) {
        if (classOf[Node].isAssignableFrom(cl)) {
          b += ModelClass(cl)
        }
      }
    }
    b.result
  }


  private[sql] def isJavaOptional(cl:Class[_]) = cl == classOf[Optional[_]]
  private[sql] def isJavaList(cl:Class[_]) = cl == classOf[java.util.List[_]]
  private[sql] def isJavaMap(cl:Class[_])  = cl == classOf[java.util.Map[_, _]]

  private[sql] def resolveParameter(p:Parameter) : ObjectType = {
    val cl = p.getType
    resolveTypeOf(p.getParameterizedType)
  }

  private[sql] def resolveTypeOf(tpe:Type) : ObjectType = {
    tpe match {
      case pt:ParameterizedType =>
        val base = resolveTypeOf(pt.getRawType)
        val args = pt.getActualTypeArguments.map { arg =>
          resolveTypeOf(arg)
        }
        GenericType(base.rawType, args)
      case cl:Class[_] =>
        ObjectType.of(cl)
    }
  }

}
