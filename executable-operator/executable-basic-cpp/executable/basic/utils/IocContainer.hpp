#ifndef IOC_CONTAINER_HPP
#define IOC_CONTAINER_HPP

#include <string>
#include <unordered_map>
#include <memory>
#include <functional>
#include <boost/any.hpp>

namespace clic {
    using std::string;
    using std::unordered_map;
    using boost::any;
    
    // class IocContainer {
    // public:
    //     IocContainer(){}
    //     ~IocContainer(){}

    //     template <class T>
    //     void RegisterType(string strKey)
    //     {
    //         typedef T* I;
    //         std::function<I()> function = Construct<I, T>::invoke;
    //         RegisterType(strKey, function);
    //     }

    //     template <class I, class T, typename... Ts>
    //     void RegisterType(string strKey)
    //     {
    //         std::function<I* (Ts...)> function = Construct<I*, T, Ts...>::invoke;
    //         RegisterType(strKey, function);
    //     }

    //     template <class I>
    //     I* Resolve(string strKey)
    //     {
    //         if (m_creatorMap.find(strKey) == m_creatorMap.end())
    //             return nullptr;

    //         any resolver = m_creatorMap[strKey];
    //         std::function<I* ()> function = boost::any_cast<std::function<I* ()>>(resolver);

    //         return function();
    //     }

    //     template <class I>
    //     std::shared_ptr<I> ResolveShared(string strKey)
    //     {
    //         auto b = Resolve<I>(strKey);

    //         return std::shared_ptr<I>(b);
    //     }

    //     template <class I, typename... Ts>
    //     I* Resolve(string strKey, Ts... Args)
    //     {
    //         if (m_creatorMap.find(strKey) == m_creatorMap.end())
    //             return nullptr;

    //         any resolver = m_creatorMap[strKey];
    //         std::function<I* (Ts...)> function = boost::any_cast<std::function<I* (Ts...)>>(resolver);

    //         return function(Args...);
    //     }

    //     template <class I, typename... Ts>
    //     std::shared_ptr<I> ResolveShared(string strKey, Ts... Args)
    //     {
    //         auto b = Resolve<I, Ts...>(strKey, Args...);

    //         return std::shared_ptr<I>(b);
    //     }

    // private:
    //     template<typename I, typename T, typename... Ts>
    //     struct Construct
    //     {
    //         static I invoke(Ts... Args) { return I(new T(Args...)); }
    //     };

    //     void RegisterType(string strKey, any constructor)
    //     {
    //         if (m_creatorMap.find(strKey) != m_creatorMap.end())
    //             throw "this key has already exist!";

    //         // m_creatorMap.insert(make_pair(strKey, constructor));
    //         m_creatorMap[strKey] = constructor;
    //     }

    // private:
    //     unordered_map<string, any> m_creatorMap;
    // };
}

#endif