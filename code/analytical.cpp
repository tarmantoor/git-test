/** @file macro/source/macro_AnalyticalDeliveryEstimate.cpp
 *  AnalyticalDeliveryEstimate macro runs the machine learned model to
 *  determine the analytical delivery estimate for an item.
 */

#include <set>
#include <boost/optional.hpp>
#include <boost/foreach.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/version.hpp>
#include <boost/serialization/array.hpp>
#include <boost/assign/list_of.hpp>
#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include "xplat/counters_stats.hpp"
#include "common/perfect_hash_map.hpp"
#include "common/prop_tree.hpp"
#include "macro/macro_includes.hpp"
#include "query_plugin/base_types_wrappers.hpp"
#include "query_plugin/allocator_types.hpp"
#include "macro/macro_operators.hpp"
#include "macro/analytical_manager.hpp"
#include "macro/delivery_estimate_utils.hpp"
#include "macro/shipping_analytical_model.hpp"
#include "macro/time_zones.hpp"

/* Counters to track model usage and behavior. */

static ebay::xplat::counters_stats::counter_registration
    test_model_counter("macro.shipping.fnf.analytical.call_test_model",
                       &ebay::xplat::counters_add_merger, true);
static ebay::xplat::counters_stats::counter_registration
    default_model_counter("macro.shipping.fnf.analytical.call_default_model",
                          &ebay::xplat::counters_add_merger, true);
static ebay::xplat::counters_stats::counter_registration
    model_result_counter("macro.shipping.fnf.analytical.model_has_result",
                         &ebay::xplat::counters_add_merger, true);
static ebay::xplat::counters_stats::counter_registration
    au_model_result_counter("macro.shipping.fnf.au.model_has_result",
                            &ebay::xplat::counters_add_merger, true);

/** @brief Key for the QA Analytical model.
 */
struct qa_model_key
{
    qa_model_key(int32_t category, int32_t service, int32_t from_zip, int32_t to_zip) :
        category(category),
        service(service),
        from_zip(from_zip),
        to_zip(to_zip)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const qa_model_key& right) const
    {
        return from_zip == right.from_zip && to_zip == right.to_zip &&
               category == right.category && service == right.service;
    }

    int32_t category;
    int32_t service;
    int32_t from_zip;
    int32_t to_zip;
};

/** @brief Define a hash_value function for qa_model_key. This is required for us
 *    to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of qa_model_key to hash.
 */
static std::size_t hash_value(const qa_model_key& key)
{
    std::size_t hash = 0;

    boost::hash_combine(hash, 256201151 * key.category);
    boost::hash_combine(hash, 334213163 * key.service);
    boost::hash_combine(hash, 417415383 * key.from_zip);
    boost::hash_combine(hash, 532999721 * key.to_zip);
    return hash;
}

typedef boost::unordered_map<qa_model_key, int32_t> qa_model_type;

/** @brief Static map to hold the QA Analytical model.
 */
const qa_model_type qa_model_map = boost::assign::map_list_of
    (qa_model_key(37908, 19, 95126, 90067), 2)
    (qa_model_key(37908, 20, 95126, 90067), 1)
    (qa_model_key(37908, 21, 95126, 90067), 1)
    (qa_model_key(37908, 3, 95126, 90067), 3)
    (qa_model_key(37908, 7, 95126, 90067), 2)
    (qa_model_key(37908, 9, 95126, 90067), 11)
    (qa_model_key(37908, 22, 95126, 90067), 3)
    (qa_model_key(37908, 23, 95126, 90067), 4)
    (qa_model_key(37908, 24, 95126, 90067), 5)
    (qa_model_key(37908, 4, 95126, 90067), 5)
    (qa_model_key(37908, 5, 95126, 90067), 3)
    (qa_model_key(37908, 1, 95126, 90067), 7)
    (qa_model_key(162917, 1, 95126, 10002), 3)
    (qa_model_key(162917, 3, 95126, 10002), 2)
    (qa_model_key(162917, 7, 95126, 10002), 1)
    (qa_model_key(162917, 8, 95126, 10002), 4)
    (qa_model_key(162917, 10, 95126, 10002), 6)
    (qa_model_key(162917, 11, 95126, 10002), 2)
    (qa_model_key(43304, 1, 95126, 96125), 7)
    (qa_model_key(43304, 3, 95126, 96125), 1)
    (qa_model_key(43304, 7, 95126, 96125), 4)
    (qa_model_key(43304, 8, 95126, 96125), 3)
    (qa_model_key(43304, 9, 95126, 96125), 3)
    (qa_model_key(43304, 10, 95126, 96125), 9)
    (qa_model_key(43304, 14, 95126, 96125), 1)
    (qa_model_key(43304, 19, 95126, 96125), 1)
    (qa_model_key(43304, 22, 95126, 96125), 3)
    (qa_model_key(42428, 1, 95126, 89412), 6)
    (qa_model_key(42428, 3, 95126, 89412), 4)
    (qa_model_key(42428, 7, 95126, 89412), 2)
    (qa_model_key(42428, 8, 95126, 89412), 3)
    (qa_model_key(42428, 10, 95126, 89412), 8)
    (qa_model_key(42428, 14, 95126, 89412), 10)
    (qa_model_key(42428, 19, 95126, 89412), 4)
    (qa_model_key(42428, 22, 95126, 89412), 3)
    (qa_model_key(169323, 1, 95126, 90067), 6)
    (qa_model_key(169323, 3, 95126, 90067), 4)
    (qa_model_key(169323, 7, 95126, 90067), 2)
    (qa_model_key(169323, 8, 95126, 90067), 3)
    (qa_model_key(169323, 10, 95126, 90067), 8)
    (qa_model_key(169323, 14, 95126, 90067), 10)
    (qa_model_key(169323, 19, 95126, 90067), 4)
    (qa_model_key(169323, 22, 95126, 90067), 3)
    (qa_model_key(50460, 1, 95126, 10002), 7)
    (qa_model_key(50460, 3, 95126, 10002), 1)
    (qa_model_key(50460, 7, 95126, 10002), 4)
    (qa_model_key(50460, 8, 95126, 10002), 3)
    (qa_model_key(50460, 9, 95126, 10002), 3)
    (qa_model_key(50460, 10, 95126, 10002), 9)
    (qa_model_key(50460, 14, 95126, 10002), 1)
    (qa_model_key(50460, 19, 95126, 10002), 1)
    (qa_model_key(50460, 22, 95126, 10002), 3);

/** @brief @a shipping_qa_model is an class to use the QA analytical model above, which
 *    does a simple lookup to cover all the necessary test cases for end-to-end testing.
 *    This model can be turned on with a query parameter.
 */
class shipping_qa_model
{
public:
    /** @brief Main entry point for the shipping_qa_model.
     *
     *  @param[in] category The item category.
     *  @param[in] service The item shipping_service.
     *  @param[in] from_zip The item origin zip.
     *  @param[in] to_zip The item destination zip.
     */
    static int32_t evaluate(int32_t category, int32_t service, int32_t from_zip,
                            int32_t to_zip)
    {
        qa_model_key key(category, service, from_zip, to_zip);
        qa_model_type::const_iterator it = qa_model_map.find(key);
        int32_t ret = -1;

        if (XPLAT_UNLIKELY(it != qa_model_map.end()))
            ret = it->second;
        return ret;
    }
};

/** @brief The @a analytical_info struct holds counts used to generate features
 *    for the analytical delivery estimate model.
 */
struct analytical_info
{
    /** @brief Constructs a @a analytical_info object.
     *    This is the default constructor.
     */
    analytical_info() :
        data()
    {
    }

    /** @brief Gets the Total field from the data array.
     */
    int16_t get_total() const
    {
        return data[0];
    }

    /** @brief Gets the field for the day of week.
     *
     *  @param day_of_week The day of the week to get from the array.
     */
    int16_t get_day(int64_t day_of_week) const
    {
        if (XPLAT_UNLIKELY(day_of_week < 1 || day_of_week > 7))
            day_of_week = 1;
        return data[day_of_week];
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in,out] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template <typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & boost::serialization::make_array(data, analytical_info_data_size);
    }

    /* Analytical data holds 1 datum per day of the week, plus one for the total. */
    static const int32_t analytical_info_data_size = 8;
    int16_t data[analytical_info_data_size];
};

/** @brief The @a shipping_zip_key struct holds the lookup key for the shipping_zip
 *    analytical map. It has a shipping method, a origin zip3 and a destination zip3.
 */
struct shipping_zip_key
{
    /** @brief Constructs a @a shipping_zip_key object. This is the default constructor.
     */
    shipping_zip_key() :
        shipping_service_id(0),
        origin_zip(0),
        dest_zip(0)
    {
    }

    /** @brief Constructs a @a shipping_zip_key object.
     *
     *  @param[in] service The shipping service id.
     *  @param[in] origin The origin zip.
     *  @param[in] dest The destination zip.
     */
    shipping_zip_key(int32_t service, int16_t origin, int16_t dest) :
        shipping_service_id(service),
        origin_zip(origin),
        dest_zip(dest)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const shipping_zip_key& right) const
    {
        return shipping_service_id == right.shipping_service_id &&
               origin_zip == right.origin_zip &&
               dest_zip == right.dest_zip;
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template <typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & shipping_service_id;
        ar & origin_zip;
        ar & dest_zip;
    }

    /** @brief Define a universal_hash function for shipping_zip_key. This is required
     *    for us to use it as a key to a perfect_hash_map.
     *
     *  @param[in] key The instance of zip_key to hash.
     *  @param[in] a The hashing paramater.
     */
    static std::size_t universal_hash(const shipping_zip_key& key, std::size_t a)
    {
        std::size_t hash = 0;

        if (a == 0)
            a = 179422921;
        boost::hash_combine(hash, a * 256201151 * key.shipping_service_id);
        boost::hash_combine(hash, a * 334213163 * key.origin_zip);
        boost::hash_combine(hash, a * 532999721 * key.dest_zip);
        return hash;
    }

    int32_t shipping_service_id;
    int16_t origin_zip;
    int16_t dest_zip;
};

/** @brief Define a hash_value function for shipping_zip_key. This is required for us
 *    to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of shipping_zip_key to hash.
 */
std::size_t hash_value(const shipping_zip_key& key)
{
    std::size_t hash = 0;

    boost::hash_combine(hash, 256201151 * key.shipping_service_id);
    boost::hash_combine(hash, 334213163 * key.origin_zip);
    boost::hash_combine(hash, 532999721 * key.dest_zip);
    return hash;
}

/** @brief The @a zip_key struct holds the lookup key for the shipping_zip
 *    analytical map. It has a shipping method, a origin zip3 and a destination zip3.
 */
struct zip_key
{
    /** @brief Constructs a @a zip_key object. This is the default constructor.
     */
    zip_key() :
        origin_zip(0),
        dest_zip(0)
    {
    }

    /** @brief Constructs a @a zip_key object.
     *
     *  @param[in] origin_zip The origin zip.
     *  @param[in] dest_zip The destination zip.
     */
    zip_key(int16_t origin_zip, int16_t dest_zip) :
        origin_zip(origin_zip),
        dest_zip(dest_zip)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const zip_key& right) const
    {
        return origin_zip == right.origin_zip &&
               dest_zip == right.dest_zip;
    }

    /** @brief Serialization function used by Boost serialization.
     *
     *  @param[in] ar The Archive to read/write to.
     *  @param[in] version Not used, but required by the interface.
     */
    template <typename A>
    void serialize(A& ar, const unsigned int version)
    {
        ar & origin_zip;
        ar & dest_zip;
    }

    /** @brief Define a universal_hash function for zip_key. This is required for us
     *    to use it as a key to a perfect_hash_map.
     *
     *  @param[in] key The instance of zip_key to hash.
     *  @param[in] a The hashing paramater.
     */
    static std::size_t universal_hash(const zip_key& key, std::size_t a)
    {
        std::size_t hash = 0;
        if (a == 0)
            a = 179422921;
        boost::hash_combine(hash, a * 334213163 * key.origin_zip);
        boost::hash_combine(hash, a * 532999721 * key.dest_zip);
        return hash;
    }

    int16_t origin_zip;
    int16_t dest_zip;
};

/** @brief Define a hash_value function for zip_key. This is required for us
 *    to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of zip_key to hash.
 */
std::size_t hash_value(const zip_key& key)
{
    std::size_t hash = 0;

    boost::hash_combine(hash, 334213163 * key.dest_zip);
    boost::hash_combine(hash, 532999721 * key.origin_zip);
    return hash;
}

/** @brief The @a zip_range_key struct holds the lookup key for the zip_range
 *    analytical map. It has a country id and a zip3 or zip.
 */
struct zip_range_key
{
    /** @brief Constructs a @a zip_range_key object.
     *    This is the default constructor.
     */
    zip_range_key() :
        country_id(0),
        zip(0)
    {
    }

    /** @brief Constructs a @a zip_range_key object.
     *
     *  @param[in] country_id The country id.
     *  @param[in] zip The zip or post code.
     */
    zip_range_key(int16_t country_id, int16_t zip) :
        country_id(country_id),
        zip(zip)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const zip_range_key& right) const
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
    int16_t zip;
};

/** @brief Define a hash_value function for zip_range_key. This is required for us
 *    to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of zip_range_key to hash.
 */
std::size_t hash_value(const zip_range_key& key)
{
    std::size_t hash = 0;

    boost::hash_combine(hash, key.country_id);
    boost::hash_combine(hash, key.zip);
    return hash;
}

/** @brief The @a service_country_key struct holds the lookup key for the
 *    service_country_range analytical map. It has a country id and service id.
 */
struct service_country_key
{
    /** @brief Constructs a @a service_country_key object.
     *    This is the default constructor.
     */
    service_country_key() :
        country_id(0),
        service_id(0)
    {
    }

    /** @brief Constructs a @a service_country_key object.
     *
     *  @param[in] country_id The country id.
     *  @param[in] service_id The shipping service id.
     */
    service_country_key(int16_t country_id, int32_t service_id) :
        country_id(country_id),
        service_id(service_id)
    {
    }

    /** @brief Equality operator, required for use as a unordered_map key.
     *
     *  @param[in] right The object to compare to for equality.
     */
    bool operator==(const service_country_key& right) const
    {
        return country_id == right.country_id &&
               service_id == right.service_id;
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
        ar & service_id;
    }

    int16_t country_id;
    int32_t service_id;
};

/** @brief Define a hash_value function for service_country_key. This is required for us
 *    to use it as a key to a boost::unordered_map.
 *
 *  @param[in] key The instance of service_country_key to hash.
 */
std::size_t hash_value(const service_country_key& key)
{
    std::size_t hash = 0;

    boost::hash_combine(hash, key.country_id);
    boost::hash_combine(hash, key.service_id);
    return hash;
}

/** @brief The @a shipping_service_est struct holds data originating from the
 *    AU DELIVYER ESTIMATE table in the Production DB for a single shipping service
 *    necessary for determining AU eBay delivery estimates for that service.
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

struct int64_hasher
{
    /** @brief Define a universal_hash function for int64_t. This is required for us
     *    to use it as a key to a perfect_hash_map.
     *
     *  @param[in] key The instance of zip_key to hash.
     *  @param[in] a The hashing paramater.
     */
    static std::size_t universal_hash(const int64_t& key, std::size_t a)
    {
        std::size_t hash = 0;

        if (a == 0)
            a = 179422921;
        boost::hash_combine(hash, a * 334213163 * key);
        return hash;
    }
};

/* Map <Service ID> to Analytical Info. */
typedef ebay::common::perfect_hash_map<int64_t, analytical_info, int64_hasher> seller_map;
/* Map <Category ID> to Analytical Info. */
typedef boost::unordered_map<int64_t, analytical_info> category_map;
/* Map <Shipping Method ID> to Analytical Info. */
typedef boost::unordered_map<int32_t, analytical_info> shipping_map;
/* Map <Shipping Method, Zip, Zip> to Analytical Info. */
typedef ebay::common::perfect_hash_map<shipping_zip_key, analytical_info,
                                       shipping_zip_key> shipping_zip_map;
/* Map <Zip, Zip> to Analytical Info. */
typedef ebay::common::perfect_hash_map<zip_key, analytical_info, zip_key> zip_map;
/* Map Zip to Zip Range. */
typedef boost::unordered_map<zip_range_key, int16_t> zip_range_map;
/* Map Service, Country to Base Service. */
typedef boost::unordered_map<service_country_key, int32_t> base_service_map;
/* Map Zip to Delivery Estimate. */
typedef boost::unordered_map<shipping_zip_key, shipping_service_est> zip_estimate_map;
/*
 * Set to hold the category level opt outs. It will likely never hold > 3 items, so a
 * std::set gives better performance than an unordered_set.
 */
typedef std::pair<int64_t, int64_t> seller_category;
typedef std::set<seller_category> category_optout_set;

static boost::scoped_ptr<zip_range_map> zip_ranges;
static boost::scoped_ptr<base_service_map> base_services;
static boost::scoped_ptr<zip_estimate_map> zip_estimates;
static boost::scoped_ptr<MACRO_NS::holiday_map> holiday_info_map;
static ebay::search::macro::eligibility_ptr eligibility;
static category_optout_set category_optouts;

/** @brief The @a experiment_model struct holds data for the experimentable
 *    analytical delivery estimate model.
 */
struct experiment_model
{
    experiment_model() :
        seller_features(),
        category_features(),
        shipping_features(),
        shipping_zip_features(),
        zip_features(),
        thresholds(),
        min_days_predicted(2),
        max_days_predicted(7)
    {
    }

    /** @brief Generates the specific name of a configuration entry, according
     *    to a prefix and an entry base name.
     *
     *  @param[in] prefix Prefix string which should be prepended to the base
     *    configuration entry name.
     *  @param[in] name Base name for the configuration entry.
     *  @return Returns the fully qualified configuration entry name.
     */
    static std::string config_entry(const char* prefix, const char* name)
    {
        return std::string(prefix) + std::string(name);
    }

    /** @brief Load the files and settings for this model
     *
     *  @param[in] ptree The property tree config.
     *  @param[in] prefix The prefix to append to config lookup strings.
     *  @param[in] is_binary Do we expect binary or text archives.
     */
    void load(const ebay::common::prop_tree& ptree, const char* prefix,
              bool is_binary)
    {
        /* Load seller historical data files. */
        std::string seller_map_path =
            ptree.get<std::string>(config_entry(prefix, "seller_history_path").c_str());

        seller_features.reset(MACRO_NS::load_serialized_data<seller_map>(
            seller_map_path.c_str(), is_binary));

        /* Load category historical data files. */
        std::string category_map_path =
            ptree.get<std::string>(config_entry(prefix, "category_history_path").c_str());

        category_features.reset(ebay::search::macro::load_map_data<category_map>(
            category_map_path.c_str(), is_binary));

        /* Load shipment historical data files. */
        std::string shipment_map_path =
            ptree.get<std::string>(config_entry(prefix, "shipment_history_path").c_str());

        shipping_features.reset(ebay::search::macro::load_map_data<shipping_map>(
            shipment_map_path.c_str(), is_binary));

        /* Load Zip historical data files. */
        std::string zip_map_path =
            ptree.get<std::string>(config_entry(prefix, "zip_history_path").c_str());

        zip_features.reset(MACRO_NS::load_serialized_data<zip_map>(
            zip_map_path.c_str(), is_binary));

        /* Load Shipment Zip historical data files. */
        std::string shipment_zip_map_path =
            ptree.get<std::string>(config_entry(prefix, "shipment_zip_history_path").c_str());

        shipping_zip_features.reset(MACRO_NS::load_serialized_data<shipping_zip_map>(
            shipment_zip_map_path.c_str(), is_binary));

        std::string macro_config_path = ptree.get<std::string>("macro_config_path");

        /* Load everything from the analytical delivery estimate json. */
        ebay::common::prop_tree macro_ptree;
        ebay::common::json_parser::read_json(macro_config_path.c_str(), macro_ptree);

        boost::optional<ebay::common::prop_tree&> opt_model_params =
            macro_ptree.get_child_optional(config_entry(prefix, "model_params").c_str());

        if (opt_model_params)
        {
            std::string thresholds_str =
                opt_model_params->get<std::string>("thresholds");
            std::vector<std::string> threshold_str_vector;

            boost::split(threshold_str_vector, thresholds_str,
                         boost::is_any_of(","));

            BOOST_FOREACH(std::string val, threshold_str_vector)
            {
                thresholds.push_back(boost::lexical_cast<double>(val));
            }

            min_days_predicted =
                boost::lexical_cast<size_t>(opt_model_params->get<std::string>("min_days_predicted"));
            max_days_predicted =
                boost::lexical_cast<size_t>(opt_model_params->get<std::string>("max_days_predicted"));
        }
    }

    /** @brief Releases memory used by this model.
     */
    void clear()
    {
        seller_features.reset();
        category_features.reset();
        shipping_features.reset();
        shipping_zip_features.reset();
        zip_features.reset();
        thresholds.clear();
    }

    boost::scoped_ptr<seller_map> seller_features;
    boost::scoped_ptr<category_map> category_features;
    boost::scoped_ptr<shipping_map> shipping_features;
    boost::scoped_ptr<shipping_zip_map> shipping_zip_features;
    boost::scoped_ptr<zip_map> zip_features;
    std::vector<double> thresholds;
    std::size_t min_days_predicted;
    std::size_t max_days_predicted;
};

static experiment_model default_model;
static experiment_model test_model;

/** @brief Translate the to_zip into the format we use.
 *
 *  @param[in] to_zip_big The full format of the to zip.
 *  @param[in] to_country_id The destination country.
 */
static int16_t translate_to_zip(int32_t to_zip_big, int32_t to_country_id)
{
    int16_t to_zip = 0;

    if (XPLAT_LIKELY(to_country_id == ebay::search::macro::country::united_states ||
                     to_country_id == ebay::search::macro::country::germany))
        to_zip = (int16_t) (to_zip_big / 100);
    else if (XPLAT_UNLIKELY(to_country_id == ebay::search::macro::country::australia))
        to_zip = (int16_t) to_zip_big;
    else if (XPLAT_UNLIKELY(to_zip >= 10000))
        to_zip = (int16_t) (to_zip_big / 100);
    else
        to_zip = (int16_t) to_zip_big;
    return to_zip;
}

/** @brief Translate the from_zip into the format we use.
 *
 *  @param[in] from_zip_string The string formatted origin zip.
 *  @param[in] to_country_id The origin country.
 */
static int16_t translate_from_zip(const QPL_NS::blob_vect& from_zip_string,
                                  int32_t from_country_id)
{
    int32_t from_zip = -1;

    if (XPLAT_LIKELY(from_zip_string.size() > 0 && from_zip_string[0].size > 0))
    {
        from_zip = 0;
        std::size_t size = from_zip_string[0].size;
        const char* from_zip_raw = from_zip_string[0].data;

        for (std::size_t i = 0; i < size && i < 4; i++)
        {
            /* Parse numeric Zips, just return whatever for non-numeric. */
            if (from_zip_raw[i] < '0' || from_zip_raw[i] > '9' ||
                (from_country_id != ebay::search::macro::country::australia && i >= 3))
                break;
            from_zip = from_zip * 10 + from_zip_raw[i] - '0';
        }
    }
    return (int16_t) from_zip;
}

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

/** @brief Set the seller map features.
 *
 *  @param[in,out] features The model feature array.
 *  @param[in] day_of_week The day of the week.
 *  @param[in] seller_id The seller id.
 *  @param[in] model The model to use.
 */
static void set_seller_features(int32_t features[MACRO_NS::ship_model::MAX_VALUE],
                                int64_t day_of_week, int64_t seller_id,
                                experiment_model& model)
{
    features[MACRO_NS::ship_model::SELLER_TOTAL_AVERAGE] = -1;
    features[MACRO_NS::ship_model::SELLER_DAY_AVERAGE] = -1;
    /* Read seller historical data. */
    if (XPLAT_LIKELY(model.seller_features != NULL))
    {
        seller_map::const_iterator it = model.seller_features->find(seller_id);

        if (XPLAT_LIKELY(it != model.seller_features->end()))
        {
            features[MACRO_NS::ship_model::SELLER_TOTAL_AVERAGE] =
                it->second.get_total();
            features[MACRO_NS::ship_model::SELLER_DAY_AVERAGE] =
                it->second.get_day(day_of_week);
        }
    }
}

/** @brief Set the category map features.
 *
 *  @param[in,out] features The model feature array.
 *  @param[in] day_of_week The day of the week.
 *  @param[in] leaf_category_id The category id.
 *  @param[in] model The model to use.
 */
static void set_category_features(int32_t features[MACRO_NS::ship_model::MAX_VALUE],
                                  int64_t day_of_week, int64_t leaf_category_id,
                                  experiment_model& model)
{
    features[MACRO_NS::ship_model::CATEGORY_TOTAL_AVERAGE] = -1;
    features[MACRO_NS::ship_model::CATEGORY_DAY_AVERAGE] = -1;
    /* Read leaf category historical data. */
    if (XPLAT_LIKELY(model.category_features != NULL))
    {
        category_map::const_iterator it =
            model.category_features->find(leaf_category_id);

        if (XPLAT_LIKELY(it != model.category_features->end()))
        {
            features[MACRO_NS::ship_model::CATEGORY_TOTAL_AVERAGE] =
                it->second.get_total();
            features[MACRO_NS::ship_model::CATEGORY_DAY_AVERAGE] =
                it->second.get_day(day_of_week);
        }
    }
}

/** @brief Set the shipping service and zip map features.
 *
 *  @param[in,out] features The model feature array.
 *  @param[in] day_of_week The day of the week.
 *  @param[in] shipping_service The shipping service.
 *  @param[in] to_zip The buyers zip location.
 *  @param[in] from_zip The item/seller zip location.
 *  @param[in] model The model to use.
 */
static void set_shipment_zip_features(int32_t features[MACRO_NS::ship_model::MAX_VALUE],
                                      int64_t day_of_week, int32_t shipping_service,
                                      int16_t to_zip, int16_t from_zip,
                                      experiment_model& model)
{
    features[MACRO_NS::ship_model::SHIPPING_METHOD_TOTAL_AVERAGE] = -1;
    features[MACRO_NS::ship_model::SHIPPING_METHOD_DAY_AVERAGE] = -1;
    features[MACRO_NS::ship_model::ZIP_TOTAL_AVERAGE] = -1;
    features[MACRO_NS::ship_model::ZIP_DAY_AVERAGE] = -1;
    features[MACRO_NS::ship_model::SHIPPING_METHOD_ZIP_TOTAL_AVERAGE] = -1;
    features[MACRO_NS::ship_model::SHIPPING_METHOD_ZIP_DAY_AVERAGE] = -1;
    /* Read shipment method historical data. */
    if (XPLAT_LIKELY(model.shipping_features != NULL))
    {
        shipping_map::const_iterator it = model.shipping_features->find(shipping_service);

        if (XPLAT_LIKELY(it != model.shipping_features->end()))
        {
            features[MACRO_NS::ship_model::SHIPPING_METHOD_TOTAL_AVERAGE] =
                it->second.get_total();
            features[MACRO_NS::ship_model::SHIPPING_METHOD_DAY_AVERAGE] =
                it->second.get_day(day_of_week);
        }
    }

    /* Read zip historical data. */
    if (XPLAT_LIKELY(model.zip_features != NULL))
    {
        zip_key key(from_zip, to_zip);

        zip_map::const_iterator it = model.zip_features->find(key);

        if (XPLAT_LIKELY(it != model.zip_features->end()))
        {
            features[MACRO_NS::ship_model::ZIP_TOTAL_AVERAGE] = it->second.get_total();
            features[MACRO_NS::ship_model::ZIP_DAY_AVERAGE] =
                it->second.get_day(day_of_week);
        }
    }

    /* Read zip historical data. */
    if (XPLAT_LIKELY(model.shipping_zip_features != NULL))
    {
        shipping_zip_key key(shipping_service, from_zip, to_zip);

        shipping_zip_map::const_iterator it = model.shipping_zip_features->find(key);

        if (XPLAT_LIKELY(it != model.shipping_zip_features->end()))
        {
            features[MACRO_NS::ship_model::SHIPPING_METHOD_ZIP_TOTAL_AVERAGE] =
                it->second.get_total();
            features[MACRO_NS::ship_model::SHIPPING_METHOD_ZIP_DAY_AVERAGE] =
                it->second.get_day(day_of_week);
        }
    }
}

/** @brief Set the shipping service and zip map features.
 *
 *  @param[in,out] min_days The min delivery estimate.
 *  @param[in,out] max_days The max delivery estimate.
 *  @param[in] shipping_service The shipping service.
 *  @param[in] to_zip The buyers zip location.
 *  @param[in] to_country_id The buyers country.
 *  @param[in] from_zip The item/seller zip location.
 *  @param[in] from_country_id The item country.
 *  @param[in] handling_time The seller's stated handling days.
 */
static void zip_to_zip_model(int32_t& min_days, int32_t& max_days,
                             int32_t shipping_service, int16_t to_zip,
                             int32_t to_country_id, int16_t from_zip,
                             int32_t from_country_id, int32_t handling_time)
{
    /* Calculate the zip->zip AU models. */
    if (XPLAT_LIKELY(base_services != NULL && zip_ranges != NULL &&
                     zip_estimates != NULL && to_zip != 0 && from_zip != 0 &&
                     shipping_service != 0 && from_country_id == to_country_id &&
                     from_country_id != 0))
    {
        service_country_key service_key((int16_t) from_country_id, shipping_service);
        base_service_map::const_iterator it = base_services->find(service_key);

        if (XPLAT_UNLIKELY(it != base_services->end()))
        {
            zip_range_key zip_to_key((int16_t) to_country_id, to_zip);
            zip_range_key zip_from_key((int16_t) from_country_id, from_zip);
            zip_range_map::const_iterator it_to = zip_ranges->find(zip_to_key);
            zip_range_map::const_iterator it_from = zip_ranges->find(zip_from_key);

            if (XPLAT_LIKELY(it_to != zip_ranges->end() &&
                             it_from != zip_ranges->end()))
            {
                shipping_zip_key lookup_key(it->second, it_to->second,
                                            it_from->second);
                zip_estimate_map::const_iterator it_estimate =
                    zip_estimates->find(lookup_key);

                if (XPLAT_LIKELY(it_estimate != zip_estimates->end() &&
                                 it_estimate->second.max_hours >= 0))
                {
                    au_model_result_counter.enabled_add_sample(1);
                    min_days = it_estimate->second.min_hours / 24 + handling_time;
                    max_days = it_estimate->second.max_hours / 24 + handling_time;
                }
            }
        }
    }
}

REGISTER_MACRO(AnalyticalDeliveryEstimate);
USING_ATTR(item:attribute:a228, ATTR_TYPE_INT32, handling_time);
USING_ATTR(item:attribute:Site, ATTR_TYPE_INT32, Site);
USING_ATTR(item:attribute:Ctry, ATTR_TYPE_INT32, Ctry);
USING_ATTR(item:attribute:ZipRegion, ATTR_TYPE_BLOB_VEC, FromZip);
USING_ATTR(item:attribute:SellerID, ATTR_TYPE_INT64, SellerId);
USING_ATTR(item:attribute:LeafCats, ATTR_TYPE_INT64_VEC, LeafCats);
USING_ATTR(item:attribute:AllCats, ATTR_TYPE_INT64_VEC, AllCats);
USING_ATTR(item:attribute:ExtraMailClassInfo, ATTR_TYPE_INT64_VEC, shipping_services);
USING_ATTR(item:attribute:NCurrentPrice, ATTR_TYPE_INT32, item_price);
USING_ATTR(synthetic:query:DestinationCountry, ATTR_TYPE_INT32, ToCtry);
USING_ATTR(synthetic:query:DestinationRegion, ATTR_TYPE_INT32, ToRegion);
USING_ATTR(synthetic:query:DestinationZip, ATTR_TYPE_INT32, ToZip);
USING_ATTR(synthetic:query:sde_model, ATTR_TYPE_STRING, sde_model);
USING_ATTR(synthetic:named_expression:CalculatedShippingCost, ATTR_TYPE_INT64_VEC, CalculatedShippingCost);
USING_ATTR(synthetic:named_expression:NxNativeDeliveryEstimate, ATTR_TYPE_INT64_VEC, NativeDeliveryEstimate);
USING_ATTR(synthetic:named_expression:NxDeliveryEstimateStartDate, ATTR_TYPE_INT64_VEC, DeliveryEstimateStartDate);
USING_ATTR(synthetic:named_expression:Distance, ATTR_TYPE_INT64, dist_val);
USING_ATTR(seller:attribute:FnfOptOut, ATTR_TYPE_INT32, FnfOptOut);
USING_ATTR(MEMALLOC_ATTRIBUTE, ATTR_TYPE_FUNCTION, MEMALLOC_FUNCTION);
USING_ATTR(MEMFREE_ATTRIBUTE, ATTR_TYPE_FUNCTION, MEMFREE_FUNCTION);

static const std::size_t nde_max_column = 1;
static const std::size_t nde_service_column = 2;
static const std::size_t nde_working_column = 3;
static const std::size_t return_size = 2;
static const std::size_t shipcalc_column_number_error = 0;
static const std::size_t shipcalc_column_number_low_cost = 5;
static const std::size_t des_column_number_start_date = 0;
static const std::size_t des_column_number_start_time = 2;

DECLARE_MACRO(AnalyticalDeliveryEstimate)
{
    int32_t min_days = -1;
    int32_t max_days = -1;
    int32_t from_country_id = attr_get__Ctry(QPL_ATTR_CTX, 0);
    int32_t to_country_id = attr_get__ToCtry(QPL_ATTR_CTX, 0);
    int32_t to_region = attr_get__ToRegion(QPL_ATTR_CTX, 0);
    int32_t to_zip_big = attr_get__ToZip(QPL_ATTR_CTX, 0);
    const QPL_NS::qpl_blob sde_model =
        attr_get__sde_model(QPL_ATTR_CTX, QPL_NS::qpl_blob());
    const QPL_NS::blob_vect from_zip_string = attr_get__FromZip(QPL_ATTR_CTX);
    const QPL_NS::int64_vect allcats_vect = attr_get__AllCats(QPL_ATTR_CTX);
    int32_t handling_time = attr_get__handling_time(QPL_ATTR_CTX, 0);
    int32_t item_price = attr_get__item_price(QPL_ATTR_CTX, 0);
    int32_t listing_site_id = attr_get__Site(QPL_ATTR_CTX, 0);
    int32_t has_opt_out = attr_get__FnfOptOut(QPL_ATTR_CTX, 0);
    const QPL_NS::qpl_int64_vect* native_estimate =
        attr_get__NativeDeliveryEstimate(QPL_ATTR_CTX);
    const QPL_NS::qpl_int64_vect* estimate_start_date =
        attr_get__DeliveryEstimateStartDate(QPL_ATTR_CTX);
    const QPL_NS::qpl_int64_vect* shipping_cost =
        attr_get__CalculatedShippingCost(QPL_ATTR_CTX);
    int64_t distance = attr_get__dist_val(QPL_ATTR_CTX, 0); /* In miles. */
    int64_t is_analytical_eligible = false;
    int64_t seller_id = attr_get__SellerId(QPL_ATTR_CTX, 0);
    int32_t shipping_service = 0;
    int16_t to_zip = 0;
    int16_t from_zip = 0;
    int64_t leaf_category_id = 0;
    int64_t shipping_price = 1;
    int64_t hour_of_day = 0;
    int64_t day_of_week = 0;
    int64_t month_of_year = 0;
    int64_t days_from_nonworking_day = 0;
    int64_t is_payment_on_holiday = 0;
    int64_t native_max = -1;
    int8_t non_working_days = 0;

    if (XPLAT_UNLIKELY(handling_time == 0))
        handling_time = 1;
    if (XPLAT_LIKELY(eligibility != NULL))
        is_analytical_eligible = eligibility->is_analytical_eligible(from_country_id,
                                                                     to_country_id,
                                                                     to_region,
                                                                     to_zip_big,
                                                                     handling_time,
                                                                     listing_site_id);
    if (XPLAT_LIKELY(native_estimate->count > nde_working_column))
    {
        shipping_service = (int32_t) native_estimate->values[nde_service_column];
        non_working_days = (int8_t) native_estimate->values[nde_working_column];
        native_max = native_estimate->values[nde_max_column];
        if (native_max < 0)
            is_analytical_eligible = false;
    }
    if (XPLAT_UNLIKELY(has_opt_out != 0))
        is_analytical_eligible = false;
    if (allcats_vect.size() > 0 && !category_optouts.empty() &&
        category_optouts.count(seller_category(seller_id, allcats_vect[0])) > 0)
        is_analytical_eligible = false;
    if (is_analytical_eligible)
    {
        if (XPLAT_LIKELY(to_zip_big != 0))
            to_zip = translate_to_zip(to_zip_big, to_country_id);
        from_zip = translate_from_zip(from_zip_string, from_country_id);
    }
    if (XPLAT_UNLIKELY(from_zip == -1))
        distance = -1;
    else
    {
        /*
         * Convert to bucketed scale.  Each bucket is 55km, rounded to nearest int.
         */
        distance = (distance * 1609 + 27500) / 55000;
    }
    if (XPLAT_UNLIKELY(is_analytical_eligible &&
                       to_country_id == ebay::search::macro::country::australia))
        zip_to_zip_model(min_days, max_days, shipping_service, to_zip, to_country_id,
                         from_zip, from_country_id, handling_time);

    /* Check the EP param to see if we should be using the QA Model. */
    if (XPLAT_UNLIKELY(is_analytical_eligible &&
        to_country_id == ebay::search::macro::country::united_states &&
        sde_model.size == 2 && std::strncmp(sde_model.data, "qa", 2) == 0))
    {
        const QPL_NS::qpl_int64_vect* attr_item_leaf_cats =
            attr_get__LeafCats(QPL_ATTR_CTX);
        int32_t from_zip_big = 0;

        if (attr_item_leaf_cats != NULL && attr_item_leaf_cats->count > 0)
            leaf_category_id = attr_item_leaf_cats->values[0];
        from_zip_big = translate_from_zip_big(from_zip_string);
        max_days = shipping_qa_model::evaluate((int32_t) leaf_category_id,
                                               shipping_service, from_zip_big,
                                               to_zip_big);
        min_days = max_days;
    }
    else if (is_analytical_eligible &&
        to_country_id == ebay::search::macro::country::united_states)
    {
        if (XPLAT_LIKELY(shipping_cost->count > shipcalc_column_number_low_cost &&
                         shipping_cost->values[shipcalc_column_number_error] == 0))
            shipping_price = shipping_cost->values[shipcalc_column_number_low_cost];
        if (XPLAT_LIKELY(estimate_start_date->count > des_column_number_start_time))
        {
            MACRO_NS::date_t start_date =
                estimate_start_date->values[des_column_number_start_date];

            hour_of_day = estimate_start_date->values[des_column_number_start_time] %
                          MACRO_NS::seconds_per_day / MACRO_NS::seconds_per_hour;
            day_of_week = (start_date + 1) % 7 + 1; /* Sun = 1, Sat = 7. */
            month_of_year = MACRO_NS::time_zone_info::get_month_from_day(start_date);

            const MACRO_NS::holiday_info* origin_holidays =
                MACRO_NS::get_holidays(from_country_id, holiday_info_map.get());

            if (XPLAT_LIKELY(origin_holidays != NULL))
            {
                is_payment_on_holiday = origin_holidays->is_holiday(start_date);
                days_from_nonworking_day = 0;

                MACRO_NS::date_t day = start_date;

                while (!MACRO_NS::holiday_info::is_non_working_day(day,
                                                                   origin_holidays,
                                                                   non_working_days))
                {
                    day++;
                    days_from_nonworking_day++;
                    if (XPLAT_UNLIKELY(days_from_nonworking_day >= 7))
                        break;
                }
            }
        }

        experiment_model* model = &default_model;

        /* If the sde_model paramater is set to model 'b', use the test model. */
        if (XPLAT_UNLIKELY(sde_model.size == 1 && sde_model.data[0] == 'b'))
        {
            model = &test_model;
            test_model_counter.enabled_add_sample(1);
        }
        else
            default_model_counter.enabled_add_sample(1);

        int32_t features[MACRO_NS::ship_model::MAX_VALUE];

        /* Setting model features. */
        features[MACRO_NS::ship_model::HOUR_OF_DAY] = (int32_t) hour_of_day;
        features[MACRO_NS::ship_model::DAY_OF_WEEK] = (int32_t) day_of_week;
        features[MACRO_NS::ship_model::MONTH_OF_YEAR] = (int32_t) month_of_year;
        features[MACRO_NS::ship_model::SHIPPING_FEE] =
            (int32_t) (shipping_price + 99) / 100;
        features[MACRO_NS::ship_model::ITEM_PRICE] = (item_price + 99) / 100;
        features[MACRO_NS::ship_model::DISTANCE] = (int32_t) distance;
        features[MACRO_NS::ship_model::HANDLING_DAYS] = handling_time;
        features[MACRO_NS::ship_model::DAYS_FROM_NONWORKING_DAYS] =
            (int32_t) days_from_nonworking_day;
        features[MACRO_NS::ship_model::IS_PAYMENT_ON_HOLIDAY] =
            (int32_t) is_payment_on_holiday;
        set_seller_features(features, day_of_week, seller_id, *model);
        set_shipment_zip_features(features, day_of_week, shipping_service, to_zip,
                                  from_zip, *model);

        const QPL_NS::qpl_int64_vect* attr_item_leaf_cats =
            attr_get__LeafCats(QPL_ATTR_CTX);

        if (attr_item_leaf_cats != NULL && attr_item_leaf_cats->count > 0)
            leaf_category_id = attr_item_leaf_cats->values[0];
        set_category_features(features, day_of_week, leaf_category_id, *model);

        double model_score = MACRO_NS::shipping_tree_model::evaluate(features);
        std::size_t max_model_days = model->max_days_predicted;

        if (XPLAT_UNLIKELY(sde_model.size == 2 && sde_model.data[0] == 'D' &&
                           sde_model.data[1] >= '0' && sde_model.data[1] <= '9'))
            max_model_days = sde_model.data[1] - '0';

        for (std::size_t i = model->min_days_predicted; i <= max_model_days; i++)
        {
            if (i >= model->thresholds.size())
                break;

            if (model_score <= model->thresholds[i])
            {
                min_days = (int32_t) i;
                max_days = (int32_t) i;
                model_result_counter.enabled_add_sample(1);
                break;
            }
        }
    }
    QPL_NS::qpl_allocator ator(QPL_APPL_CTX, QPL_ATTR_CTX);
    QPL_NS::qpl_int64_vect* return_vect = (QPL_NS::qpl_int64_vect*)
        ator.alloc(sizeof(QPL_NS::qpl_int64_vect) +
                   return_size * sizeof(int64_t));

    return_vect->count = 0;
    return_vect->values[return_vect->count++] = min_days;
    return_vect->values[return_vect->count++] = max_days;
    QPL_RETVAL->type = QPL_NS::ATTR_TYPE_INT64_VEC;
    QPL_RETVAL->value.int64_vect_v = return_vect;
}

/** @brief Resets all of the macro's static pointers */
static void cleanup()
{
    eligibility.reset();
    holiday_info_map.reset();
    default_model.clear();
    test_model.clear();
    zip_ranges.reset();
    base_services.reset();
    zip_estimates.reset();
    category_optouts.clear();
}

DECLARE_MACRO_INIT(AnalyticalDeliveryEstimate_init)
{
    try
    {
        /*
         * The implicit 'cfg_ptree' instantiated by the DECLARE_MACRO_INIT macro
         * holds a const reference to a Boost property tree object.
         * Basically: const ebay::common::prop_tree& cfg_ptree
         */
        boost::optional<const ebay::common::prop_tree&> opt_AnalyticalDeliveryEstimate =
            cfg_ptree.get_child_optional("AnalyticalDeliveryEstimate");

        if (opt_AnalyticalDeliveryEstimate &&
            opt_AnalyticalDeliveryEstimate->get<bool>("enabled"))
        {
            bool is_binary = true;
            boost::optional<bool> is_text_archive =
                opt_AnalyticalDeliveryEstimate->get_optional<bool>("is_text_archive");

            if (is_text_archive && *is_text_archive)
                is_binary = false;

            eligibility = MACRO_NS::analytical_manager::load_eligibility(cfg_ptree);

            std::string holiday_map_path =
                opt_AnalyticalDeliveryEstimate->get<std::string>("shipping_service_holiday_path");

            holiday_info_map.reset(
                ebay::search::macro::load_map_data<MACRO_NS::holiday_map>(
                    holiday_map_path.c_str(), is_binary));

            /*
             * Start loading the model features
             */
            default_model.load(*opt_AnalyticalDeliveryEstimate, "", is_binary);

            std::string zip_ranges_map_path =
                opt_AnalyticalDeliveryEstimate->get<std::string>("zip_ranges_path");
            std::string base_services_map_path =
                opt_AnalyticalDeliveryEstimate->get<std::string>("base_services_path");
            std::string zip_estimates_map_path =
                opt_AnalyticalDeliveryEstimate->get<std::string>("zip_estimates_path");

            zip_ranges.reset(ebay::search::macro::load_map_data<zip_range_map>(
                zip_ranges_map_path.c_str(), is_binary));
            base_services.reset(ebay::search::macro::load_map_data<base_service_map>(
                base_services_map_path.c_str(), is_binary));
            zip_estimates.reset(ebay::search::macro::load_map_data<zip_estimate_map>(
                zip_estimates_map_path.c_str(), is_binary));

            std::string macro_config_path =
                opt_AnalyticalDeliveryEstimate->get<std::string>("macro_config_path");

            /* Load everything from the analytical delivery estimate json. */
            ebay::common::prop_tree macro_ptree;

            ebay::common::json_parser::read_json(macro_config_path.c_str(), macro_ptree);

            boost::optional<bool> test_enabled =
                macro_ptree.get_optional<bool>("test_enabled");

            if (test_enabled && *test_enabled)
                test_model.load(*opt_AnalyticalDeliveryEstimate, "ep_", is_binary);

            boost::optional<ebay::common::prop_tree&> category_opt_outs =
                macro_ptree.get_child_optional("category_opt_outs");

            if (category_opt_outs)
            {
                BOOST_FOREACH(ebay::common::prop_tree_entry& i, *category_opt_outs)
                {
                    if (*i.get_name() == '#')
                        continue;

                    int64_t seller_id = boost::lexical_cast<int64_t>(i.get_name());
                    std::string categories_str = i.get<std::string>();
                    std::vector<std::string> categories;

                    boost::split(categories, categories_str, boost::is_any_of(","));

                    BOOST_FOREACH(std::string category, categories)
                    {
                        category_optouts.insert(seller_category(seller_id,
                                                boost::lexical_cast<int64_t>(category)));
                    }
                }
            }
        }
    }
    catch (...)
    {
        /* Always cleanup if the initialization failed. */
        cleanup();
        XPLAT_RETHROW();
    }
}

DECLARE_MACRO_CLEANUP(AnalyticalDeliveryEstimate_cleanup)
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
