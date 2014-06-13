/** @file macro/source/macro_NativeDeliveryEstimate.cpp
 *  NativeDeliveryEstimate macro reads in global tables with shipping service
 *  information and uses that along with data from the index to compute the
 *  native eBay delivery estimate for an item.
 */

#include <map>
#include <vector>
#include <fstream>
#include <iostream>
#include <bitset>
#include <boost/optional.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/version.hpp>
#include <boost/serialization/bitset.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>
#include "common/json_parser.hpp"
#include "macro/macro_includes.hpp"
#include "query_plugin/base_types_wrappers.hpp"
#include "query_plugin/allocator_types.hpp"
#include "macro/delivery_estimate_utils.hpp"
#include "xplat/path.hpp"

/** @brief The @a shipping_service_est struct holds data originating from the
 *    POSTALCODE SHIPPING ESTIMATES table in the Production DB
 *    necessary for determining if the buyer's location falls in the exclusion zones.
 */
struct shipping_service_est
{
    /** @brief Constructs a @a shipping_service_est object.
     *    This is the default constructor.
     */
    shipping_service_est() :
        min_hours(-1),
        max_hours(-1)
    {
    }

    /** @brief Constructs a @a shipping_service_est object.
     *    This is the explicit constructor.
     *
     *  @param[in] min_hours Minimum delivery estimate time in hours.
     *  @param[in] max_hours Maximum delivery estimate time in hours.
     */
    shipping_service_est(int16_t min_hours, int16_t max_hours) :
        min_hours(min_hours),
        max_hours(max_hours)
    {
    }

    /** @brief Constructs a @a shipping_service_est object.
     *    This is the copy constructor.
     */
    shipping_service_est(const shipping_service_est& copy) :
        min_hours(copy.min_hours),
        max_hours(copy.max_hours)
    {
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in,out] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template <typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & min_hours;
        ar & max_hours;
    }

    /* Min Delivery Time in Hours for this service. */
    int16_t min_hours;
    /* Max Delivery Time in Hours for this service. */
    int16_t max_hours;
};

/** @brief The @a shipping_service_info struct holds data originating from the
 *    SHIPPING_SERVICE table in the Production DB for a single shipping service
 *    necessary for determining native eBay delivery estimates for that service.
 */
struct shipping_service_info
{
    /** @brief Constructs a @a shipping_service_info object.
     *    This is the default constructor.
     */
    shipping_service_info() :
        min_hours(-1),
        max_hours(-1),
        working_days_flags(0)
    {
    }

    /** @brief Constructs a @a shipping_service_info object.
     *    This is the explicit constructor.
     *
     *  @param[in] min Minimum delivery estimate time in hours.
     *  @param[in] max Maximum delivery estimate time in hours.
     *  @param[in] flags Working Day flags for this service.
     */
    shipping_service_info(int16_t min, int16_t max, int8_t flags) :
        min_hours(min),
        max_hours(max),
        working_days_flags(flags)
    {
    }

    /** @brief Constructs a @a shipping_service_info object.
     *    This is the copy constructor.
     */
    shipping_service_info(const shipping_service_info& copy) :
        min_hours(copy.min_hours),
        max_hours(copy.max_hours),
        working_days_flags(copy.working_days_flags)
    {
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in,out] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template <typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & min_hours;
        ar & max_hours;
        ar & working_days_flags;
    }

    /* Min Delivery Time in Hours for this service. */
    int16_t min_hours;
    /* Max Delivery Time in Hours for this service. */
    int16_t max_hours;
    /** @brief Flag field denoting which days of the week are holidays.
     *    0x40 is Sunday, 0x1 is Saturday, etc.
     */
    int8_t working_days_flags;
};

/** @brief The @a cbt_key struct holds the lookup key for cross border trade
 *    shipping service estimates. The data originates in the SHIPPING_SERVICE_ESTIMATE
 *    table in the Production DB, and has <service, origin, dest> as a key.
 */
struct cbt_key
{
    /** @brief Constructs a @a cbt_key object.
     *    This is the default constructor.
     */
    cbt_key() :
        shipping_service_id(0),
        origin_country_id(0),
        dest_country_id(0)
    {
    }

    /** @brief Constructs a @a cbt_key object.
     *
     *  @param[in] service The shipping service id.
     *  @param[in] origin The origin country id.
     *  @param[in] dest The destination country id.
     */
    cbt_key(int32_t service, int16_t origin, int16_t dest) :
        shipping_service_id(service),
        origin_country_id(origin),
        dest_country_id(dest)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const cbt_key& right) const
    {
        return shipping_service_id == right.shipping_service_id &&
               origin_country_id == right.origin_country_id &&
               dest_country_id == right.dest_country_id;
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template<typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & shipping_service_id;
        ar & origin_country_id;
        ar & dest_country_id;
    }

    int32_t shipping_service_id;
    int16_t origin_country_id;
    int16_t dest_country_id;
};

/** @brief Define a hash_value function for cbt_key. This is required for us
 *    to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of cbt_key to hash.
 */
static std::size_t hash_value(const cbt_key& key)
{
    std::size_t hash = 0;
    boost::hash_combine(hash, 256201151 * key.shipping_service_id);
    boost::hash_combine(hash, 334213163 * key.origin_country_id);
    boost::hash_combine(hash, 532999721 * key.dest_country_id);
    return hash;
}

/** @brief The @a zip_range_key struct holds the lookup key for the z2z_range_map
 *    map. It has a country id and zip.
 */
struct z2z_range_key
{
    /** @brief Constructs a @a zip_range_key object.
     *    This is the default constructor.
     */
    z2z_range_key() :
        country_id(0),
        zip(0)
    {
    }

    /** @brief Constructs a @a z2z_range_key object.
     *
     *  @param[in] country_id The country id.
     *  @param[in] zip The zip or post code.
     */
    z2z_range_key(int16_t country_id, int32_t zip) :
        country_id(country_id),
        zip(zip)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const z2z_range_key& right) const
    {
        return country_id == right.country_id && zip == right.zip;
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template <typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & country_id;
        ar & zip;
    }

    int16_t country_id;
    int32_t zip;
};

/** @brief Define a hash_value function for z2z_range_key. This is required for us
 *    to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of z2z_range_key to hash.
 */
std::size_t hash_value(const z2z_range_key& key)
{
    std::size_t hash = 0;
    boost::hash_combine(hash, key.country_id);
    boost::hash_combine(hash, key.zip);
    return hash;
}

/** @brief The @a z2z_tozipnull_key struct holds the lookup key for z2z_tozipnull_map.
 *    It has from country id, to country id, sender zip and shipping service.
 */
struct z2z_tozipnull_key
{
    /** @brief Constructs a @a z2z_tozipnull_key object.
     *    This is the default constructor.
     */
    z2z_tozipnull_key() :
       from_country_id(0),
       to_country_id(0),
       from_zip_hash(0),
       shipping_service_id(0)
    {
    }

    /** @brief Constructs a @a z2z_tozipnull_key object.
     *
     *  @param[in] from_country The from country id id.
     *  @param[in] to_country The to country Id.
     *  @param[in] from_zip The sender postal code.
     *  @param[in] service The shipping service id.
     */
    z2z_tozipnull_key(int16_t from_country, int16_t to_country,
                      int32_t from_zip, int32_t service) :
        from_country_id(from_country),
        to_country_id(to_country),
        from_zip_hash(from_zip),
        shipping_service_id(service)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const z2z_tozipnull_key& right) const
    {
        return from_country_id == right.from_country_id &&
        to_country_id == right.to_country_id &&
        from_zip_hash == right.from_zip_hash &&
        shipping_service_id == right.shipping_service_id;
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template<typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & from_country_id;
        ar & to_country_id;
        ar & from_zip_hash;
        ar & shipping_service_id;
    }

    int16_t from_country_id;
    int16_t to_country_id;
    int32_t from_zip_hash;
    int32_t shipping_service_id;
};

/** @brief Define a hash_value function for z2z_tozipnull_key. This is required
 *    for us to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of z2z_tozipnull_key to hash.
 */
std::size_t hash_value(const z2z_tozipnull_key& key)
{
    std::size_t hash = 0;
    boost::hash_combine(hash, 532999721 * key.from_country_id);
    boost::hash_combine(hash, 256201151 * key.to_country_id);
    boost::hash_combine(hash, 532999721 * key.shipping_service_id);
    boost::hash_combine(hash, key.from_zip_hash);
    return hash;
}

/** @brief The @a z2z_default_key struct holds the lookup key for z2z_default_map.
 *    It has from country id, to country id, sender zip, buyer zip and shipping service.
 */
struct z2z_default_key
{
    /** @brief Constructs a @a z2z_default_key object.
     *    This is the default constructor.
     */
    z2z_default_key() :
       from_country_id(0),
       to_country_id(0),
       from_zip_hash(0),
       to_zip_hash(0),
       shipping_service_id(0)
    {
    }

    /** @brief Constructs a @a z2z_default_key object.
     *
     *  @param[in] from_country The shipping service id.
     *  @param[in] to_country The country Id.
     *  @param[in] from_zip The sender postal code.
     *  @param[in] to_zip The buyer postal code.
     *  @param[in] service The shipping service id.
     */
    z2z_default_key(int16_t from_country, int16_t to_country, int32_t from_zip,
                    int32_t to_zip, int32_t service) :
        from_country_id(from_country),
        to_country_id(to_country),
        from_zip_hash(from_zip),
        to_zip_hash(to_zip),
        shipping_service_id(service)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const z2z_default_key& right) const
    {
        return from_country_id == right.from_country_id && to_country_id == right.to_country_id &&
        from_zip_hash == right.from_zip_hash && to_zip_hash == right.to_zip_hash &&
        shipping_service_id == right.shipping_service_id;
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template<typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & shipping_service_id;
        ar & from_country_id;
        ar & to_country_id;
        ar & from_zip_hash;
        ar & to_zip_hash;
    }

    int16_t from_country_id;
    int16_t to_country_id;
    int32_t from_zip_hash;
    int32_t to_zip_hash;
    int32_t shipping_service_id;
};

/** @brief Define a hash_value function for z2z_default_key. This is required
 *    for us to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of z2z_default_key to hash.
 */
std::size_t hash_value(const z2z_default_key& key)
{
    std::size_t hash = 0;
    boost::hash_combine(hash, 532999721 * key.from_country_id);
    boost::hash_combine(hash, 256201151 * key.to_country_id);
    boost::hash_combine(hash, 532999721 * key.shipping_service_id);
    boost::hash_combine(hash, key.from_zip_hash);
    boost::hash_combine(hash, key.to_zip_hash);
    return hash;
}

/** @brief The @a exclusion_zip_key struct holds the lookup key for exc_map.
 *    It has shipping service, country id, buyer zip.
 */
struct exclusion_zip_key
{
    /** @brief Constructs a @a exc_zip_key object.
     *    This is the default constructor.
     */
    exclusion_zip_key() :
        shipping_service_id(0),
        zip_code_hash(0),
        country_id(0)
    {
    }

    /** @brief Constructs a @a exc_zip_key object.
     *
     *  @param[in] service The shipping service id.
     *  @param[in] country The country Id.
     *  @param[in] zip The zipcode.
     */
    exclusion_zip_key(int32_t service, int16_t country, int32_t zip) :
        shipping_service_id(service),
        zip_code_hash(zip),
        country_id(country)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const exclusion_zip_key& right) const
    {
        return shipping_service_id == right.shipping_service_id &&
               country_id == right.country_id && zip_code_hash == right.zip_code_hash;
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template<typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & shipping_service_id;
        ar & country_id;
        ar & zip_code_hash;
    }

    int32_t shipping_service_id;
    int32_t zip_code_hash;
    int16_t country_id;
};

/** @brief Define a hash_value function for exclusion_zip_key. This is required
 *    for us to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of exclusion_zip_key to hash.
 */
std::size_t hash_value(const exclusion_zip_key& key)
{
    std::size_t hash = 0;
    boost::hash_combine(hash, 532999721 * key.shipping_service_id);
    boost::hash_combine(hash, 256201151 * key.country_id);
    boost::hash_combine(hash, key.zip_code_hash);
    return hash;
}

/** @brief The @a z2z_services_key struct holds the lookup key for z2z_services_map.
 *    It has from country id, to country id and shipping service.
 */
struct z2z_services_key
{
    /** @brief Constructs a @a z2z_services_key object.
     *    This is the default constructor.
     */
    z2z_services_key() :
        from_country_id(0),
        to_country_id(0),
        shipping_service_id(0)
    {
    }

   /** @brief Constructs a @a z2z_services_key object.
     *
     *  @param[in] from_country The from country id id.
     *  @param[in] to_country The to country Id.
     *  @param[in] service The shipping service id.
     */
    z2z_services_key(int16_t from_country, int16_t to_country, int32_t service) :
        from_country_id(from_country),
        to_country_id(to_country),
        shipping_service_id(service)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const z2z_services_key& right) const
    {
        return from_country_id == right.from_country_id &&
               to_country_id == right.to_country_id &&
               shipping_service_id == right.shipping_service_id;
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template<typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & from_country_id;
        ar & to_country_id;
        ar & shipping_service_id;
    }

    int16_t from_country_id;
    int16_t to_country_id;
    int32_t shipping_service_id;
};

/** @brief Define a hash_value function for z2z_services_key. This is required
 *    for us to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of z2z_services_key to hash.
 */
std::size_t hash_value(const z2z_services_key& key)
{
    std::size_t hash = 0;
    boost::hash_combine(hash, 532999721 * key.from_country_id);
    boost::hash_combine(hash, 256201151 * key.to_country_id);
    boost::hash_combine(hash, 532999721 * key.shipping_service_id);
    return hash;
}

/* Map Shipping Service ID to Shipping Service Info. */
typedef boost::unordered_map<int32_t, shipping_service_info> ssi_map;
/* Map <Service ID, Origin, Destination> to Shipping Service Info. */
typedef boost::unordered_map<cbt_key, shipping_service_info> cbt_map;
/* Map Country ID, Postal Code, Shipping Service Id to Exclusion Zones info. */
typedef boost::unordered_map<exclusion_zip_key, shipping_service_est> exc_map;
/* Map From Country Id, To Country ID, From Zip, To Zip, Shipping Service Id to Shipping Service Info. */
typedef boost::unordered_map<z2z_default_key, shipping_service_est> z2z_default_map;
/* Map Country Id, Postal code to all Postal codes in that range. */
typedef boost::unordered_map<z2z_range_key, int32_t> z2z_range_map;
/* Map From Country Id, To Country ID, From Zip, Shipping Service Id to Shipping Service Info. */
typedef boost::unordered_map<z2z_tozipnull_key, shipping_service_est> z2z_tozipnull_map;
/* Map From Country Id, To Country ID, From Zip, To Zip, Shipping Service Id to Shipping Service Info. */
typedef boost::unordered_map<z2z_default_key, shipping_service_est> z2z_estimate_map;

typedef boost::unordered_set<z2z_services_key> z2z_services_set;

/* Static map to hold the shipping service info. */
static boost::scoped_ptr<ssi_map> service_info_map;
/* Static map to hold the cbt shipping service info. */
static boost::scoped_ptr<cbt_map> service_cbt_map;
/* Static map to hold Exclusion Zones info. */
static boost::scoped_ptr<exc_map> service_exc_map;
/* Static map to hold Zip2Zip ranges data info for DE and AU. */
static boost::scoped_ptr<z2z_range_map> service_z2z_range_map;
/* Static map to hold Zip2Zip data. */
static boost::scoped_ptr<z2z_default_map> service_z2z_default_map;
/* Static map to hold Zip2Zip buyer zip null data for DE. */
static boost::scoped_ptr<z2z_tozipnull_map> service_z2z_tozipnull_map;
/* Static map to hold Zip2Zip ranges estimates for DE and AU. */
static boost::scoped_ptr<z2z_estimate_map> service_z2z_estimate_map;

static boost::scoped_ptr<z2z_services_set> service_z2z_services_set;
static const int32_t uk_zip_base = 36;

/** @brief Translate the full numeric from_zip into an int.
 *
 *  @param[in] from_zip_string The string formatted origin zip.
 */
static int32_t translate_from_zip_big(const QPL_NS::blob_vect& from_zip_string)
{
    int32_t from_zip = 0;

    if (XPLAT_LIKELY(from_zip_string.size() > 0 && from_zip_string[0].size > 0))
    {
        std::size_t size = from_zip_string[0].size;
        const char* from_zip_raw = from_zip_string[0].data;

        for (std::size_t i = 0; i < size && i < 9; i++)
        {
            /* Parse numeric Zips, just return whatever for non-numeric. */
            if (from_zip_raw[i] < '0' || from_zip_raw[i] > '9')
                break;
            from_zip = from_zip * 10 + from_zip_raw[i] - '0';
        }
    }
    return from_zip;
}

/** @brief Get an estimate from the z2z default map if it exists.
 *
 *  @param[in] from_country_id the origin country
 *  @param[in] to_country_id the destination country
 *  @param[in] from_zip the origin postcode
 *  @param[in] to_zip the destination postcode
 *  @param[in] shipping_service the shipping service being used
 */
boost::optional<shipping_service_est> get_z2z_default(int16_t from_country_id,
       int16_t to_country_id, int32_t from_zip, int32_t to_zip, int32_t shipping_service)
{
    int32_t from_ctry_base = 10;
    int32_t to_ctry_base = 10;

    /*
     * If to_country is UK, use 36 as the base for longest prefix match
     * otherwise use 10.
     */
    if (XPLAT_UNLIKELY(from_country_id == MACRO_NS::country::united_kingdom))
        from_ctry_base = uk_zip_base;
    if (XPLAT_UNLIKELY(to_country_id == MACRO_NS::country::united_kingdom))
        to_ctry_base = uk_zip_base;

    boost::optional<shipping_service_est> est;
    int32_t temp_from_zip = from_zip;
    int32_t temp_to_zip = to_zip;

    while (true)
    {
        temp_to_zip = to_zip;
        while (true)
        {
            z2z_default_key key(from_country_id, to_country_id, temp_from_zip,
                                temp_to_zip, shipping_service);
            z2z_default_map::const_iterator it;

            it = service_z2z_default_map->find(key);
            if (XPLAT_UNLIKELY(it != service_z2z_default_map->end()))
            {
                std::cout << "Default --- found YOYOYOYO \n";
                est = it->second;
                return est;
            }
            temp_to_zip /= to_ctry_base;
            if (temp_to_zip == 0)
                break;
        }
        temp_from_zip /= from_ctry_base;
        if (temp_from_zip == 0)
            break;
    }
    return est;
}

/** @brief Get an estimate from the z2z ranges map if it exists.
 *
 *  @param[in] from_country_id the origin country
 *  @param[in] to_country_ip the destination country
 *  @param[in] from_zip the origin postcode
 *  @param[in] to_zip_big the destination postcode
 *  @param[in] shipping_service the shipping service being used
 */
boost::optional<shipping_service_est> get_z2z_ranges(int16_t from_country_id, int16_t to_country_id,
                       int32_t from_zip, int32_t to_zip, int32_t shipping_service)
{
    int32_t from_ctry_base = 10;
    int32_t to_ctry_base = 10;
    /*
     * If to_country is UK, use 36 as the base for longest prefix match
     * otherwise use 10.
     */
    if (XPLAT_UNLIKELY(from_country_id == MACRO_NS::country::united_kingdom))
        from_ctry_base = uk_zip_base;
    if (XPLAT_UNLIKELY(to_country_id == MACRO_NS::country::united_kingdom))
        to_ctry_base = uk_zip_base;

    boost::optional<shipping_service_est> est;
    z2z_range_map::const_iterator it_range;
    it_range =  service_z2z_range_map->begin();
    int32_t temp_from_zip = from_zip;
    int32_t temp_to_zip = to_zip;

    while (temp_from_zip > 0)
    {
        temp_to_zip = to_zip;
        z2z_range_key zip_from_key(from_country_id, temp_from_zip);
        z2z_range_map::const_iterator it_from = service_z2z_range_map->find(zip_from_key);
        if (XPLAT_LIKELY(it_from != service_z2z_range_map->end()))
        {
            while (temp_to_zip > 0)
            {
                z2z_range_key zip_to_key(to_country_id, temp_to_zip);
                z2z_range_map::const_iterator it_to = service_z2z_range_map->find(zip_to_key);

                if (XPLAT_LIKELY(it_to != service_z2z_range_map->end()))
                {
                    z2z_default_key lookup_key(from_country_id, to_country_id, it_from->second,
                                            it_to->second, shipping_service);
                    z2z_estimate_map::const_iterator it_estimate =
                                 service_z2z_estimate_map->find(lookup_key);

                    if (XPLAT_UNLIKELY(it_estimate != service_z2z_estimate_map->end() &&
                                 it_estimate->second.max_hours >= 0))
                    {
                        std::cout << "Ranges --- found YOYOYOYO \n";
                        est = it_estimate->second;
                        return est;
                    }
                }
                temp_to_zip /= to_ctry_base;
            }
        }
        temp_from_zip /= from_ctry_base;
    }
    return est;
}

/** @brief Get an estimate from the z2z null map if it exists.
 *
 *  @param[in] from_country_id the origin country
 *  @param[in] to_country_id the destination country
 *  @param[in] from_zip the origin postcode
 *  @param[in] shipping_service the shipping service being used
 */
boost::optional<shipping_service_est> get_z2z_tozipnull(int16_t from_country_id,
              int16_t to_country_id, int32_t from_zip, int32_t shipping_service)
{
    int32_t from_ctry_base = 10;

    /*
     * If to_country is UK, use 36 as the base for longest prefix match
     * otherwise use 10.
     */
    if (XPLAT_UNLIKELY(from_country_id == MACRO_NS::country::united_kingdom))
        from_ctry_base = uk_zip_base;

    boost::optional<shipping_service_est> est;
    int32_t temp_from_zip = from_zip;
    z2z_tozipnull_map::const_iterator it;

    while (temp_from_zip > 0)
    {
        z2z_tozipnull_key key(from_country_id, to_country_id, temp_from_zip, shipping_service);

        it = service_z2z_tozipnull_map->find(key);
        if (XPLAT_UNLIKELY(it != service_z2z_tozipnull_map->end()))
        {
            std::cout << "Buyer zip null --- found YOYOYOYO \n";
            est = it->second;
            return est;
        }
        temp_from_zip /= from_ctry_base;
    }
    return est;
}

/** @brief Get an estimate from the exclusion zone map if it exists.
 *
 *  @param[in] to_country_id the destination country
 *  @param[in] to_zip the destination postcode
 *  @param[in] shipping_service the shipping service being used
 */
boost::optional<shipping_service_est> get_exc_est(int16_t to_country_id, int32_t to_zip,
                                                  int32_t shipping_service)
{
    int32_t to_ctry_base = 10;

    /*
     * If to_country is UK, use 36 as the base for longest prefix match
     * otherwise use 10.
     */
    if (XPLAT_UNLIKELY(to_country_id == MACRO_NS::country::united_kingdom))
        to_ctry_base = uk_zip_base;

    boost::optional<shipping_service_est> est;
    exc_map::const_iterator it;
    int32_t temp_to_zip = to_zip;

    while (temp_to_zip > 0)
    {
        exclusion_zip_key key(shipping_service, to_country_id, temp_to_zip);

        it = service_exc_map->find(key);
        if (XPLAT_UNLIKELY(it != service_exc_map->end()))
        {
            est = it->second;
            return est;
        }
        temp_to_zip /= to_ctry_base;
    }
    return est;
}

/** @brief Get an estimate from the z2z model .
 *
 *  @param[in] from_country_id the origin country
 *  @param[in] to_country_ip the destination country
 *  @param[in] from_zip the origin postcode
 *  @param[in] to_zip_big the destination postcode
 *  @param[in] shipping_service the shipping service being used
 */
boost::optional<shipping_service_est> get_z2z_est(int16_t from_country_id, int16_t to_country_id,
        int32_t from_zip, int32_t to_zip, int32_t shipping_service)
{
    from_country_id = 77;
    shipping_service = 7712;
    from_zip = 9545;
    to_zip = 7575;
    to_country_id = 77;
    z2z_services_set::const_iterator it;
    boost::optional<shipping_service_est> z2z_est;

    z2z_services_key key(from_country_id, to_country_id, shipping_service);
    it = service_z2z_services_set->find(key);
    if (XPLAT_UNLIKELY(it != service_z2z_services_set->end()))
    {
        if (XPLAT_UNLIKELY(!z2z_est && service_z2z_default_map != NULL))
            z2z_est = get_z2z_default(from_country_id, to_country_id, from_zip,
                                      to_zip, shipping_service);
        if (XPLAT_UNLIKELY(!z2z_est && service_z2z_range_map != NULL &&
                                    service_z2z_estimate_map != NULL))
            z2z_est = get_z2z_ranges(from_country_id, to_country_id, from_zip,
                                     to_zip, shipping_service);
        if (XPLAT_UNLIKELY(!z2z_est && service_z2z_tozipnull_map != NULL))
            z2z_est = get_z2z_tozipnull(from_country_id, to_country_id, from_zip,
                                        shipping_service);
        if (XPLAT_UNLIKELY(!z2z_est && service_exc_map != NULL))
            z2z_est = get_exc_est(to_country_id, to_zip, shipping_service);
    }
    else
        std::cout << "not a z2z service \n";
    return z2z_est;
}

REGISTER_MACRO(NativeDeliveryEstimate);
USING_ATTR(item:attribute:ExtraMailClassInfo, ATTR_TYPE_INT64_VEC, shipping_services);
USING_ATTR(item:attribute:Ctry, ATTR_TYPE_INT32, Ctry);
USING_ATTR(item:attribute:a228, ATTR_TYPE_INT32, handling_time);
USING_ATTR(synthetic:query:DestinationCountry, ATTR_TYPE_INT32, ToCtry);
USING_ATTR(synthetic:query:DestinationZip, ATTR_TYPE_INT32, ToZip);
USING_ATTR(item:attribute:ZipRegion, ATTR_TYPE_BLOB_VEC, FromZip);
USING_ATTR(synthetic:named_expression:CalculatedShippingCost, ATTR_TYPE_INT64_VEC, CalculatedShippingCost);
USING_ATTR(MEMALLOC_ATTRIBUTE, ATTR_TYPE_FUNCTION, MEMALLOC_FUNCTION);
USING_ATTR(MEMFREE_ATTRIBUTE, ATTR_TYPE_FUNCTION, MEMFREE_FUNCTION);

static const std::size_t shipcalc_column_number_error = 0;
static const std::size_t shipcalc_column_number_mail_class = 2;
static const int64_t cbt_shipping_service_id = 50000;
static const std::size_t return_size = 4;

DECLARE_MACRO(NativeDeliveryEstimate)
{
    int32_t handling_time = attr_get__handling_time(QPL_ATTR_CTX, 0);
    int16_t from_country_id = (int16_t) MACRO_NS::convert_country(
        attr_get__Ctry(QPL_ATTR_CTX, 0));
    int16_t to_country_id = (int16_t) MACRO_NS::convert_country(
        attr_get__ToCtry(QPL_ATTR_CTX, 0));
    const QPL_NS::qpl_int64_vect* shipping_services_vect =
        attr_get__shipping_services(QPL_ATTR_CTX);
    const QPL_NS::qpl_int64_vect* shipping_cost =
        attr_get__CalculatedShippingCost(QPL_ATTR_CTX);
    const QPL_NS::blob_vect from_zip_string = attr_get__FromZip(QPL_ATTR_CTX);
    int32_t to_zip_big = attr_get__ToZip(QPL_ATTR_CTX, 0);
    int32_t shipping_service = 0;
    int16_t max_hours = -1;
    int16_t min_hours = -1;
    int8_t working_days = 0x41; /*1000001*/
    bool have_z2z_est = false;
    bool is_cbt = false;
    from_country_id = 77;

    if (from_country_id != to_country_id)
        is_cbt = true;

    if (XPLAT_LIKELY(shipping_cost->count > shipcalc_column_number_mail_class &&
                     shipping_cost->values[shipcalc_column_number_error] == 0))
        shipping_service = static_cast<int32_t>
            (shipping_cost->values[shipcalc_column_number_mail_class]);
    else if (shipping_services_vect != NULL && shipping_services_vect->count > 0)
    {
        /*
         * We didn't get a ShipCalc response, so attempt to figure out the
         * proper shipping service.
         */
        for (std::size_t i = 0; i < shipping_services_vect->count; i++)
        {
            if (is_cbt && shipping_services_vect->values[i] >= cbt_shipping_service_id)
            {
                shipping_service = (int32_t) shipping_services_vect->values[i];
                break;
            }
            else if (!is_cbt &&
                     shipping_services_vect->values[i] < cbt_shipping_service_id)
            {
                shipping_service = (int32_t) shipping_services_vect->values[i];
                break;
            }
        }
    }

    if (XPLAT_LIKELY(!is_cbt && service_z2z_services_set != NULL))
    {
        int32_t from_zip_big = 0;

        from_zip_big = translate_from_zip_big(from_zip_string);
        boost::optional<shipping_service_est> z2z_est = get_z2z_est(from_country_id, to_country_id,
                                            from_zip_big, to_zip_big, shipping_service);
        if (XPLAT_UNLIKELY(z2z_est))
        {
            std :: cout << "z2z Estimate found ***********\n";
            max_hours = z2z_est->max_hours;
            min_hours = z2z_est->min_hours;
            std :: cout << "Max  " << max_hours << "  Min  " << min_hours << std :: endl;
            have_z2z_est = true;
        }
        else
            std :: cout << "z2z Estimate not found ***********\n";
    }

    if (XPLAT_LIKELY(service_info_map != NULL && shipping_service != 0 &&
                     !have_z2z_est))
    {
        ssi_map::const_iterator it = service_info_map->find(shipping_service);

        if (XPLAT_LIKELY(it != service_info_map->end()))
        {
            max_hours = it->second.max_hours;
            min_hours = it->second.min_hours;
            working_days = it->second.working_days_flags;
        }
    }

    if (XPLAT_UNLIKELY((shipping_service >= cbt_shipping_service_id ||
                        from_country_id != to_country_id) && !have_z2z_est))
    {
        if (XPLAT_LIKELY(service_cbt_map != NULL))
        {
            max_hours = -1;
            min_hours = -1;

            cbt_key key(shipping_service, from_country_id, to_country_id);
            cbt_map::const_iterator it = service_cbt_map->find(key);

            /*
             * With the current CBT service estimates, this is unlikely, but
             * if we add a lot more estimates, we should change this to likely.
             */
            if (XPLAT_UNLIKELY(it != service_cbt_map->end()))
            {
                max_hours = it->second.max_hours;
                min_hours = it->second.min_hours;
            }
            else
            {
                /* Didn't find (from,to), try (to,to). */
                cbt_key key2(shipping_service, to_country_id, to_country_id);

                it = service_cbt_map->find(key2);
                if (XPLAT_UNLIKELY(it != service_cbt_map->end()))
                {
                    max_hours = it->second.max_hours;
                    min_hours = it->second.min_hours;
                }
            }
        }
    }

    int64_t min_days = -1;
    int64_t max_days = -1;

    if (handling_time == 0)
        handling_time = 1;
    /* Calculate the business days. */
    if (max_hours >= 0 && handling_time > 0)
        max_days = static_cast<int64_t>(max_hours / 24) + handling_time;
    if (min_hours >= 0 && handling_time > 0)
        min_days = static_cast<int64_t>(min_hours / 24) + handling_time;

    QPL_NS::qpl_allocator ator(QPL_APPL_CTX, QPL_ATTR_CTX);
    QPL_NS::qpl_int64_vect* return_vect = (QPL_NS::qpl_int64_vect*)
        ator.alloc(sizeof(QPL_NS::qpl_int64_vect) + return_size * sizeof(int64_t));

    return_vect->count = 0;
    return_vect->values[return_vect->count++] = min_days;
    return_vect->values[return_vect->count++] = max_days;
    return_vect->values[return_vect->count++] = static_cast<int64_t>(shipping_service);
    return_vect->values[return_vect->count++] = static_cast<int64_t>(working_days);
    QPL_RETVAL->type = QPL_NS::ATTR_TYPE_INT64_VEC;
    QPL_RETVAL->value.int64_vect_v = return_vect;
}

/** @brief Resets all of the macro's static pointers */
static void cleanup()
{
    service_info_map.reset();
    service_cbt_map.reset();
    service_exc_map.reset();
    service_z2z_default_map.reset();
    service_z2z_range_map.reset();
    service_z2z_tozipnull_map.reset();
    service_z2z_estimate_map.reset();
}

DECLARE_MACRO_INIT(NativeDeliveryEstimate_init)
{
    try
    {
        /*
         * The implicit 'cfg_ptree' instantiated by the DECLARE_MACRO_INIT macro
         * holds a const reference to a Boost property tree object.
         * Basically: const ebay::common::prop_tree& cfg_ptree
         */
        boost::optional<const ebay::common::prop_tree&> opt_NativeDeliveryEstimate =
            cfg_ptree.get_child_optional("NativeDeliveryEstimate");

        if (opt_NativeDeliveryEstimate &&
            opt_NativeDeliveryEstimate->get<bool>("enabled"))
        {
            bool is_binary = true;
            boost::optional<bool> is_text_archive =
                opt_NativeDeliveryEstimate->get_optional<bool>("is_text_archive");

            if (is_text_archive && *is_text_archive)
                is_binary = false;

            ebay::xplat::path ssi_map_path =
                opt_NativeDeliveryEstimate->get<std::string>(
                "shipping_service_info_path");
            ebay::xplat::path cbt_map_path =
                opt_NativeDeliveryEstimate->get<std::string>("shipping_cbt_path");
            ebay::xplat::path macro_config_path =
                opt_NativeDeliveryEstimate->get<std::string>("macro_config_path");
            boost::optional<std::string> exc_map_path_str =
                opt_NativeDeliveryEstimate->get_optional<std::string>("exc_map_path");
            boost::optional<std::string> z2z_default_map_path_str =
                opt_NativeDeliveryEstimate->get_optional<std::string>("z2z_default_map_path");
            boost::optional<std::string> z2z_range_map_path_str =
                opt_NativeDeliveryEstimate->get_optional<std::string>("z2z_range_map_path");
            boost::optional<std::string> z2z_tozipnull_map_path_str =
                opt_NativeDeliveryEstimate->get_optional<std::string>("z2z_tozipnull_map_path");
            boost::optional<std::string> z2z_estimate_map_path_str =
                opt_NativeDeliveryEstimate->get_optional<std::string>("z2z_estimate_map_path");
            boost::optional<std::string> z2z_services_set_path_str =
                opt_NativeDeliveryEstimate->get_optional<std::string>("z2z_services_set_path");

            /* Load our index files. */
            service_info_map.reset(ebay::search::macro::load_map_data<ssi_map>(
                ssi_map_path.c_str(), is_binary));
            service_cbt_map.reset(ebay::search::macro::load_map_data<cbt_map>(
                cbt_map_path.c_str(), is_binary));
            if (exc_map_path_str)
            {
                ebay::xplat::path exc_map_path = exc_map_path_str.get();

                service_exc_map.reset(ebay::search::macro::load_map_data<exc_map>(
                    exc_map_path.c_str(), is_binary));
            }
            if (z2z_default_map_path_str)
            {
                ebay::xplat::path z2z_default_map_path = z2z_default_map_path_str.get();

                service_z2z_default_map.reset(ebay::search::macro::load_map_data<z2z_default_map>(
                    z2z_default_map_path.c_str(), is_binary));
            }
            if (z2z_range_map_path_str)
            {
                ebay::xplat::path z2z_range_map_path = z2z_range_map_path_str.get();

                service_z2z_range_map.reset(ebay::search::macro::load_map_data<z2z_range_map>(
                    z2z_range_map_path.c_str(), is_binary));
            }
            if (z2z_tozipnull_map_path_str)
            {
                ebay::xplat::path z2z_tozipnull_map_path = z2z_tozipnull_map_path_str.get();

                service_z2z_tozipnull_map.reset(ebay::search::macro::load_map_data<z2z_tozipnull_map>(
                    z2z_tozipnull_map_path.c_str(), is_binary));
            }
            if (z2z_estimate_map_path_str)
            {
                ebay::xplat::path z2z_estimate_map_path = z2z_estimate_map_path_str.get();

                service_z2z_estimate_map.reset(ebay::search::macro::load_map_data<z2z_estimate_map>(
                    z2z_estimate_map_path.c_str(), is_binary));
            }
            if (z2z_services_set_path_str)
            {
                ebay::xplat::path z2z_services_set_path = z2z_services_set_path_str.get();

                service_z2z_services_set.reset(ebay::search::macro::load_set_data<z2z_services_set>(
                              z2z_services_set_path.c_str(), is_binary));
            }

            /* Load everything from the index package json. */
            ebay::common::prop_tree macro_ptree;

            ebay::common::json_parser::read_json(macro_config_path.c_str(), macro_ptree);
        }
    }
    catch (...)
    {
        /* Always cleanup if the initialization failed. */
        cleanup();
        throw;
    }
}

DECLARE_MACRO_CLEANUP(NativeDeliveryEstimate_cleanup)
{
    /*
     * Always release ALL the memory acquired at initialization time.
     * Cleanup functions might be called even when the corresponding init function
     * has not been called.  This is because, being init and cleanup function not
     * logically coupled, in case one of the init functions fails (throws), the
     * engine does not know which cleanup code to run, and so it will run them all.
     */
    cleanup();
}
