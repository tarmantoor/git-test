//g++ -Wall -O2 -c filecreationtool.cpp; g++ -O2 filecreationtool.o -o filecreationtool -lboost_serialization; ./filecreationtool

/*
SQL Query for Generic Services:
SELECT CASE WHEN GENERIC_TYPE = 7 THEN 14
						WHEN GENERIC_TYPE = 8 THEN 18
						WHEN GENERIC_TYPE = 9 THEN 1
						WHEN GENERIC_TYPE = 10 THEN 7
						WHEN GENERIC_TYPE = 11 THEN 2
						WHEN GENERIC_TYPE = 12 THEN 2
						WHEN GENERIC_TYPE = 14 THEN 2
						ELSE GENERIC_TYPE END AS GENERIC_SERVICE
,SHIPPING_SERVICE_ID
FROM SHIPPING_SERVICE
WHERE IS_ENABLED = 1
AND DEPRECATE_EFFECTIVE_DATE IS NULL
AND GENERIC_TYPE IS NOT NULL
AND (GENERIC_TYPE = 1 OR
GENERIC_TYPE = 2 OR
GENERIC_TYPE = 7 OR
GENERIC_TYPE = 8 OR
GENERIC_TYPE = 9 OR
GENERIC_TYPE = 10 OR
GENERIC_TYPE = 11 OR
GENERIC_TYPE = 12 OR
GENERIC_TYPE = 14)
*/

/*
SQL Query For Shipping_services:
SELECT SHIPPING_SERVICE_ID,
COALESCE(MIN_DELIVERY_TIME_HOURS,-1),
COALESCE(MAX_DELIVERY_TIME_HOURS,-1),
CASE WHEN FLAGS = 8192 THEN 64
WHEN FLAGS = 532480 OR FLAGS = 532840 THEN 65
ELSE 0 END AS FLAGS
FROM SHIPPING_SERVICE
*/

/*
SQL Query For CBT:
SELECT SHIPPING_SERVICE_ID,ORIGIN_COUNTRY_ID,DESTINATION_COUNTRY_ID,MIN_DELIVERY_TIME_HOURS,MAX_DELIVERY_TIME_HOURS FROM SHIPPING_SERVICE_ESTIMATE
*/

/*
SELECT SHIPPING_SERVICE_ID,
SOURCE_START_ZIPCODE,
DESTINATION_START_ZIPCODE,
MIN_DELIVERY_ESTIMATE_HOURS,
MAX_DELIVERY_ESTIMATE_HOURS
FROM SHIPPING_DELIVERY_EST_LKP

*/


#include <map>
#include <set>
#include <vector>
#include <fstream>
#include <iostream>
#include <bitset>
#include <boost/optional.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/version.hpp>
#include <boost/serialization/bitset.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/gregorian/greg_calendar.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/assign/list_of.hpp>
#include "perfect_hash_map.hpp"



/** @brief 
* This function will serialize a boost::unordered_map
*/
template <class Archive, class Type, class Key, class Hash, class Compare, class Allocator>
inline void save_archive(
Archive& ar,
const boost::unordered_map<Key, Type, Hash, Compare, Allocator>& t)
{
	boost::serialization::stl::save_collection<
	Archive,
	boost::unordered_map<Key, Type, Hash, Compare, Allocator> >(ar, t);
}

/** @brief 
* This function will serialize a boost::unordered_set
*/
template<class Archive, class Key, class Compare, class Allocator >
inline void serialize_set(
    Archive & ar,
    const boost::unordered_set<Key, Compare, Allocator> &t
){
    boost::serialization::stl::save_collection<
        Archive, boost::unordered_set<Key, Compare, Allocator> 
    >(ar, t);
}

struct shipping_service_info
{
	/** @brief Constructs a @a shipping_service_info object. 
	*         This is the default constructor.
	*/
	shipping_service_info() :
	min_hours(-1),
	max_hours(-1), 
	working_days_flags(0)
	{
	}

	/** @brief Constructs a @a shipping_service_info object. 
	*         This is the explicit constructor.
	*  @param[in] min Minimum Delivery Estimate time in hours.
	*  @param[in] max Maximum Delivery Estimate time in hours.
	*  @param[in] flags Working Day flags for this service.
	*/
	shipping_service_info(int16_t min, int16_t max, int8_t flags) :
	min_hours(min),
	max_hours(max),
	working_days_flags(flags)
	{
	}

	/** @brief Constructs a @a shipping_service_info object. 
	*         This is the copy constructor.
	*/
	shipping_service_info(const shipping_service_info &copy) :
	min_hours(copy.min_hours),
	max_hours(copy.max_hours),
	working_days_flags(copy.working_days_flags)
	{
	}

	/** @brief Serialization function used by Boost serialization. 
	*  @param[in] ar The Archive to read/write to.
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
	*         0x40 is Sunday, 0x1 is Saturday, etc.
	*/
	int8_t working_days_flags;
};

/* 366*2.5 = 915 days are stored for the holiday bitset.
* So that even if we don't update for a full year, we
* still have at least several months data.
*/
static const int32_t max_holiday_bits = 915;

/* We use a 64-bit int as our Julian Day date. */
typedef int64_t date_t;

/** @brief The @a holiday_info struct holds holiday data originating from the 
*         EXCLUSION_LIST_DAY table in the Production DB. It is meant to be 
*         keyed on Country, and holds the holidays internally as a bitset.
*         Note that unlike the EXCLUSION_LIST_DAY table, which is keyed 
*         on "ListID", we assume it's been translated to the CountryId
*/
struct holiday_info
{
	/** @brief Constructs a @a holiday_info object.
	*         This is the default constructor.
	*/
	holiday_info() :
	holidays(),
	start_date(0)
	{
	}

	/** @brief Constructs a @a holiday_info object.
	*         This is the explicit constructor.
	*  @param[in] start_date Date stored as the first of the two years of holidays.
	*/
	holiday_info(date_t start) :
	holidays(),
	start_date(start)
	{
	}

	/** @brief Serialization function used by Boost serialization. 
	*  @param[in] ar The Archive to read/write to.
	*  @param[in] version Not used, but required by the interface.
	*/
	template<typename A>
	void serialize(A& ar, const unsigned int version)
	{
		ar & start_date;
		ar & holidays;
	}
	
	/** @brief Gets the correct bit for a given date.
	*  @param[in] date Julian Day to check for holiday status
	*  @return Returns @a true if date is a holiday, @a false otherwise.
	*/
	bool get_bit(date_t date) const
	{
		if ((date - start_date < 0 || date - start_date >= max_holiday_bits))
		return false;
		return holidays[date - start_date];
	}

	/** @brief Sets the correct bit for a given date. Used only during initilization.
	*  @param[in] year Year of the date to set as a holiday.
	*  @param[in] month Month of the date to set as a holiday.
	*  @param[in] day Day of the date to set as a holiday.
	*/
	void set_bit(int16_t year, int16_t month, int16_t day)
	{
		if ((month < 1 || month > 12 || day < 1 || day > 31))
		return;
		int64_t date = static_cast<int64_t>(boost::gregorian::gregorian_calendar::day_number
		(boost::gregorian::date::ymd_type(year, month, day)));

		if ((date - start_date < 0 || date - start_date >= max_holiday_bits))
		return;
		holidays.set(date - start_date);
	}

	/** @brief Gets whether the given date is a holiday.
	*         Equivalent to get_bit(), but with a friendlier name.
	*  @param[in] date Julian Day to check for holiday status
	*  @return Returns @a true if date is a holiday, @a false otherwise.
	*/
	bool is_holiday(date_t date) const
	{
		return get_bit(date);
	}

	/*
	* Holidays are represented as a bitset, with one bit per day, 
	* starting with start_day at bit zero, 366*2.5 = 915 days are stored.
	*/
	std::bitset<max_holiday_bits> holidays;
	/*
	* We hold at most 3 calendar years of holidays, -1 year from when
	* the structure was created, and +1.5 years.  Holidays are typically not 
	* defined in the DB past the next calendar year anyways.  start_date is the 
	* first day held in the structure.
	*/
	date_t start_date;
};

struct cbt_key
{
	/** @brief Default Constructor for cbt_key
	*
	*  @param[in] service The shipping service id.
	*  @param[in] origin The origin country id.
	*  @param[in] dest The destination country id.
	*/
	cbt_key(int32_t service, int32_t origin, int32_t dest) :
	shipping_service_id(service),
	origin_country_id(origin),
	dest_country_id(dest)
	{
	}

	/** @brief Equality operator.
	*
	*  @param[in] right The object to compare to for equality.
	*/
	bool operator==(const cbt_key& right) const
	{
		return shipping_service_id == right.shipping_service_id &&
		origin_country_id == right.origin_country_id &&
		dest_country_id == right.dest_country_id;
	}

	bool operator!=(const cbt_key& right) const
	{
		return !operator==(right);
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
std::size_t hash_value(const cbt_key& key)
{
	std::size_t hash = 0;
	boost::hash_combine(hash, key.shipping_service_id);
	boost::hash_combine(hash, key.origin_country_id);
	boost::hash_combine(hash, key.dest_country_id);
	return hash;
}

/** @brief The @a shipping_zip_key struct holds the lookup key for the shipping_zip
*    analytical map. It has a shipping method, a origin zip3 and a destination zip3.
*/
struct shipping_zip_key
{
	/** @brief Constructs a @a shipping_zip_key object.
	*    This is the default constructor.
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

	bool operator!=(const shipping_zip_key& right) const
	{
		return !operator==(right);
	}

	/** @brief Define a universal_hash function for shipping_zip_key. This is required for us
	*    to use it as a key to a perfect_hash_map.
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

	/** @brief Serialization function used by Boost serialization.
	*
	*  @param[in] ar The Archive to read/write to.
	*  @param[in] version Not used, but required by the interface.
	*/
	template<typename A>
	void serialize(A& ar, const unsigned int version)
	{
		ar & shipping_service_id;
		ar & origin_zip;
		ar & dest_zip;
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
	boost::hash_combine(hash, 532999721 * key.shipping_service_id);
	boost::hash_combine(hash, 256201151 * key.origin_zip);
	boost::hash_combine(hash, 334213163 * key.dest_zip);
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
	*  @param[in] country The country id.
	*  @param[in] zipcode The zip or post code.
	*/
	zip_range_key(int32_t country, int16_t zipcode) :
	country_id(country),
	zip(zipcode)
	{
	}

	/** @brief Equality operator, required for use as a unordered_map key.
	*
	*  @param[in] right The object to compare to for equality.
	*/
	bool operator==(const zip_range_key& right) const
	{
		return country_id == right.country_id &&
		zip == right.zip;
	}

	/** @brief Serialization function used by Boost serialization.
	*
	*  @param[in] ar The Archive to read/write to.
	*  @param[in] version Not used, but required by the interface.
	*/
	template<typename A>
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
	*  @param[in] country The country id.
	*  @param[in] service_id The shipping service id.
	*/
	service_country_key(int32_t country, int32_t service) :
	country_id(country),
	service_id(service)
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
	template<typename A>
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
	*  @param[in] min Minimum delivery estimate time in hours.
	*  @param[in] max Maximum delivery estimate time in hours.
	*/
	shipping_service_est(int16_t min, int16_t max) :
	min_hours(min),
	max_hours(max)
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

/* Map Country ID to Holiday List for that Country. */
typedef boost::unordered_map<int32_t, holiday_info> holiday_map;
/* Map Shipping Service ID to Shipping Service Info. */
typedef boost::unordered_map<int32_t, shipping_service_info> ssi_map;
/* Map <Service ID, Origin, Destination> to Shipping Service Info. */
typedef boost::unordered_map<cbt_key, shipping_service_info > cbt_map;
/* Map Zip to Zip Range */
typedef boost::unordered_map<zip_range_key, int16_t> zip_range_map;
/* Map Service, Country to Base Service. */
typedef boost::unordered_map<service_country_key, int32_t> base_service_map;
/* Map Zip to Delivery Estimate */
typedef boost::unordered_map<shipping_zip_key, shipping_service_est> zip_estimate_map;
/* Map <Country ID, Postal Code, Shipping Service Id> to Exclusion Zones info */
typedef boost::unordered_map<exclusion_zip_key, shipping_service_est> exc_map;
/* Map <Country ID, Postal Code, Shipping Service Id> to Exclusion Zones info */
typedef boost::unordered_map<z2z_default_key, shipping_service_est> z2z_default_map;
/* Map Country Id, Postal code to all Postal codes in that range. */
typedef boost::unordered_map<z2z_range_key, int32_t> z2z_range_map;
/* Map From Country Id, To Country ID, From Zip, Shipping Service Id to Shipping Service Info. */
typedef boost::unordered_map<z2z_tozipnull_key, shipping_service_est> z2z_tozipnull_map;
/* Map From Country Id, To Country ID, From Zip, To Zip, Shipping Service Id to Shipping Service Info. */
typedef boost::unordered_map<z2z_default_key, shipping_service_est> z2z_estimate_map;
/* Set with From Country Id, To Country ID, Shipping Service Id as Key. */
typedef boost::unordered_set<z2z_services_key> z2z_services_set;
static const int32_t UK_ZIP_BASE = 36;
static const int32_t UK_ZIP_VAR = 55;

bool is_number(const std::string& str)
{
	std::string::const_iterator it = str.begin();
	while (it != str.end() && std::isdigit(*it)) ++it;
	return !str.empty() && it == str.end();
}

int32_t power(int32_t base, int32_t pow)
{
	int32_t result = 1;
	while (pow)
	{
		if (pow & 1)
		result *= base;
		pow >>= 1;
		base *= base;
	}
	return result;
}

std :: string convert_hash_to_zip(int32_t zip)
{	
	int32_t rem = 0;
	std::string zip_code;
	std::stringstream ss;
	while (zip != 0)
	{
		rem = zip % UK_ZIP_BASE;
		if(rem >=0 && rem <=9)
		ss << rem;
		else
		ss << (char) (rem + UK_ZIP_VAR);
		zip /= UK_ZIP_BASE;
	}
	zip_code = ss.str();
	std :: reverse(zip_code.begin(), zip_code.end());
	return zip_code;
}

int32_t convert_zip_to_hash(const std :: string& zip)
{
	int32_t zip_hash = 0;
	if (is_number(zip))
	{
		zip_hash = boost::lexical_cast<int32_t>(zip);
		return zip_hash;
	}
	
	int32_t counter = static_cast<int32_t>(zip.length());
	std::string::const_iterator it;	
	
	for(it = zip.begin(); it != zip.end(); it++)
	{
		if(std :: isdigit(*it))
		zip_hash +=  ((int32_t)(*it) - '0') * power(UK_ZIP_BASE, --counter); 
		else
		zip_hash += ((int32_t)(*it) - UK_ZIP_VAR) * power(UK_ZIP_BASE, --counter);	  	    
	}
	return zip_hash;
}

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void z2z_services_create_map_data(const char* input,const char* output)
{
    std::ifstream ifs(input);

    if (!ifs)
        return;

    int16_t from_country_id;
    int16_t to_country_id;
    int32_t shipping_service;
    z2z_services_set* bset = new z2z_services_set();

    while (!ifs.fail() && !ifs.eof())
    {
        ifs >> from_country_id >> to_country_id >> shipping_service;
        z2z_services_key key(from_country_id, to_country_id, shipping_service);
        bset->insert(key);
    }
    
    std::ofstream ofs(output, std::ios_base::binary);
    boost::archive::binary_oarchive oarc(ofs);
    std::string out_text = output;
    out_text += ".txt";
    std::ofstream ofs_text(out_text.c_str());
    boost::archive::text_oarchive oarc_text(ofs_text);
    serialize_set(oarc,*bset);
    serialize_set(oarc_text,*bset);
    delete bset;
    bset=NULL;
}

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void z2zdefault_create_map_data(const char* input,const char* output)
{
    std::ifstream ifs(input);

    if (!ifs)
        return;

    int16_t from_country_id;
    int16_t to_country_id;
    int32_t shipping_service;
    int16_t min_hours;
    int16_t max_hours;    
    int32_t from_zip_hash;
    int32_t to_zip_hash;
    std :: string from_zip;
    std :: string to_zip;
    z2z_default_map* bmap = new z2z_default_map();

    while (!ifs.fail() && !ifs.eof())
    {
        ifs >> from_country_id >> to_country_id >> from_zip >> to_zip >> shipping_service >> min_hours >> max_hours;
        from_zip_hash = convert_zip_to_hash(from_zip);
        to_zip_hash = convert_zip_to_hash(to_zip);
        z2z_default_key key(from_country_id, to_country_id, from_zip_hash, to_zip_hash,shipping_service);
        shipping_service_est val(min_hours,max_hours);
        bmap->insert(std::pair<z2z_default_key, shipping_service_est>(key,val));
    }
    
    std::ofstream ofs(output, std::ios_base::binary);
    boost::archive::binary_oarchive oarc(ofs);
    std::string out_text = output;
    out_text += ".txt";
    std::ofstream ofs_text(out_text.c_str());
    boost::archive::text_oarchive oarc_text(ofs_text);
    save_archive(oarc,*bmap);
    save_archive(oarc_text,*bmap);
    delete bmap;
    bmap=NULL;
}

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void z2ztozipnull_create_map_data(const char* input,const char* output)
{
    std::ifstream ifs(input);

    if (!ifs)
        return;

    int16_t from_country_id;
    int16_t to_country_id;
    int32_t shipping_service;
    int16_t min_hours;
    int16_t max_hours;    
    int32_t from_zip_hash;
    std :: string from_zip;
    z2z_tozipnull_map* bmap = new z2z_tozipnull_map();

    while (!ifs.fail() && !ifs.eof())
    {
        ifs >> from_country_id >> to_country_id >> from_zip >> shipping_service >> min_hours >> max_hours;
        from_zip_hash = convert_zip_to_hash(from_zip);
        z2z_tozipnull_key key(from_country_id, to_country_id, from_zip_hash,shipping_service);
        shipping_service_est val(min_hours,max_hours);
        bmap->insert(std::pair<z2z_tozipnull_key, shipping_service_est>(key,val));
    }
    
    std::ofstream ofs(output, std::ios_base::binary);
    boost::archive::binary_oarchive oarc(ofs);
    std::string out_text = output;
    out_text += ".txt";
    std::ofstream ofs_text(out_text.c_str());
    boost::archive::text_oarchive oarc_text(ofs_text);
    save_archive(oarc,*bmap);
    save_archive(oarc_text,*bmap);
    delete bmap;
    bmap=NULL;
}

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void z2zranges_create_map_data(const char* input,const char* output)
{
    std::ifstream ifs(input);

    if (!ifs)
        return;

    int16_t country;
    int32_t zip_begin;
    int32_t zip_end;
    z2z_range_map* bmap = new z2z_range_map();

    while (!ifs.fail() && !ifs.eof())
    {
        ifs >> country >> zip_begin >> zip_end ;
        for(int32_t i = zip_begin; i<= zip_end; i++)
        {
            z2z_range_key temp(country,i);
            bmap->insert(std::pair<z2z_range_key, int32_t>(temp,zip_begin));
        }
    }

    std::ofstream ofs(output, std::ios_base::binary);
    boost::archive::binary_oarchive oarc(ofs);
    std::string out_text = output;
    out_text += ".txt";
    std::ofstream ofs_text(out_text.c_str());
    boost::archive::text_oarchive oarc_text(ofs_text);

    save_archive(oarc,*bmap);
    save_archive(oarc_text,*bmap);
    delete bmap;
    bmap=NULL;
}

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void z2z_create_map_data(const char* input,const char* output)
{
    std::ifstream ifs(input);

    if (!ifs)
        return;

    int16_t from_country_id;
    int16_t to_country_id;
    int32_t shipping_service;
    int16_t min_hours;
    int16_t max_hours;    
    int32_t from_zip;
    int32_t to_zip;
    z2z_estimate_map* bmap = new z2z_estimate_map();

    while (!ifs.fail() && !ifs.eof())
    {
        ifs >> from_country_id >> to_country_id >> from_zip >> to_zip >> shipping_service >> min_hours >> max_hours;
        z2z_default_key key(from_country_id, to_country_id, from_zip, to_zip,shipping_service);
        shipping_service_est val(min_hours,max_hours);
        bmap->insert(std::pair<z2z_default_key, shipping_service_est>(key,val));
    }
    
    std::ofstream ofs(output, std::ios_base::binary);
    boost::archive::binary_oarchive oarc(ofs);
    std::string out_text = output;
    out_text += ".txt";
    std::ofstream ofs_text(out_text.c_str());
    boost::archive::text_oarchive oarc_text(ofs_text);
    save_archive(oarc,*bmap);
    save_archive(oarc_text,*bmap);
    delete bmap;
    bmap=NULL;
}

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void exc_create_map_data(const char* input,const char* output)
{
    std::ifstream ifs(input);

    if (!ifs)
        return;

    int16_t country;
    int32_t shipping_service;
    int16_t min_hours;
    int16_t max_hours;    
    int32_t zip_code_hash;
    std :: string zip;
    exc_map* bmap = new exc_map();

    while (!ifs.fail() && !ifs.eof())
    {
        ifs >> country >> shipping_service >> zip >> min_hours >> max_hours;
        zip_code_hash = convert_zip_to_hash(zip);
        exclusion_zip_key temp(shipping_service,country,zip_code_hash);
        shipping_service_est temp2(min_hours,max_hours);
        bmap->insert(std::pair<exclusion_zip_key, shipping_service_est>(temp,temp2));
    }
    
    std::ofstream ofs(output, std::ios_base::binary);
    boost::archive::binary_oarchive oarc(ofs);
    std::string out_text = output;
    out_text += ".txt";
    std::ofstream ofs_text(out_text.c_str());
    boost::archive::text_oarchive oarc_text(ofs_text);

    save_archive(oarc,*bmap);
    save_archive(oarc_text,*bmap);
    delete bmap;
    bmap=NULL;
}


/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void ze_create_map_data(const char* input,const char* output)
{
	std::ifstream ifs(input);

	if (!ifs)
	return;

	int32_t shipping_service;
	int16_t min_hours;
	int16_t max_hours;
	int16_t origin_zip;
	int16_t dest_zip;
	zip_estimate_map* bmap = new zip_estimate_map();

	while (!ifs.fail() && !ifs.eof())
	{
		ifs >> shipping_service >> origin_zip >> dest_zip >> min_hours >> max_hours;
		shipping_zip_key temp(shipping_service,origin_zip,dest_zip);
		shipping_service_est temp2(min_hours,max_hours);
		bmap->insert(std::pair<shipping_zip_key, shipping_service_est>(temp,temp2));
	}

	std::ofstream ofs(output, std::ios_base::binary);
	boost::archive::binary_oarchive oarc(ofs);
	std::string out_text = output;
	out_text += ".txt";
	std::ofstream ofs_text(out_text.c_str());
	boost::archive::text_oarchive oarc_text(ofs_text);

	save_archive(oarc,*bmap);
	save_archive(oarc_text,*bmap);
	delete bmap;
	bmap=NULL;
}
/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void sb_create_map_data(const char* input,const char* output)
{
	std::ifstream ifs(input);

	if (!ifs)
	return;

	int16_t country;
	int32_t service;
	int32_t base_service;
	base_service_map* bmap = new base_service_map();

	while (!ifs.fail() && !ifs.eof())
	{
		ifs >> country >> service >> base_service ;
		service_country_key temp(country,service);
		bmap->insert(std::pair<service_country_key, int32_t>(temp,base_service));
	}

	std::ofstream ofs(output, std::ios_base::binary);
	boost::archive::binary_oarchive oarc(ofs);
	std::string out_text = output;
	out_text += ".txt";
	std::ofstream ofs_text(out_text.c_str());
	boost::archive::text_oarchive oarc_text(ofs_text);

	save_archive(oarc,*bmap);
	save_archive(oarc_text,*bmap);
	delete bmap;
	bmap=NULL;
}

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for uni ttesting 
*/
static void zr_create_map_data(const char* input,const char* output, std::set<int16_t> excluded)
{
	std::ifstream ifs(input);

	if (!ifs)
	return;

	int16_t country;
	int16_t zip_begin;
	int16_t zip_end;
	zip_range_map* bmap = new zip_range_map();

	while (!ifs.fail() && !ifs.eof())
	{
		ifs >> country >> zip_begin >> zip_end ;
		for(int16_t i = zip_begin; i<= zip_end; i++)
		{
			if(excluded.count(i)>0) continue;
			zip_range_key temp(country,i);
			bmap->insert(std::pair<zip_range_key, int16_t>(temp,zip_begin));
		}
	}

	std::ofstream ofs(output, std::ios_base::binary);
	boost::archive::binary_oarchive oarc(ofs);
	std::string out_text = output;
	out_text += ".txt";
	std::ofstream ofs_text(out_text.c_str());
	boost::archive::text_oarchive oarc_text(ofs_text);

	save_archive(oarc,*bmap);
	save_archive(oarc_text,*bmap);
	delete bmap;
	bmap=NULL;
}

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void ssi_create_map_data(const char* input,const char* output)
{
	std::ifstream ifs(input);

	if (!ifs)
	return;

	int32_t shipping_service;
	int16_t min_hours;
	int16_t max_hours;
	int16_t working_days;
	ssi_map* bmap = new ssi_map();

	while (!ifs.fail() && !ifs.eof())
	{
		ifs >> shipping_service >> min_hours >> max_hours >> working_days;
		shipping_service_info temp(min_hours,max_hours,(int8_t)working_days);
		bmap->insert(std::pair<int32_t, shipping_service_info>(shipping_service,temp));
	}

	std::ofstream ofs(output, std::ios_base::binary);
	boost::archive::binary_oarchive oarc(ofs);
	std::string out_text = output;
	out_text += ".txt";
	std::ofstream ofs_text(out_text.c_str());
	boost::archive::text_oarchive oarc_text(ofs_text);

	save_archive(oarc,*bmap);
	save_archive(oarc_text,*bmap);
	delete bmap;
	bmap=NULL;
}

typedef boost::unordered_multimap<int32_t,int32_t> gentype;

/** @brief
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void cbt_create_map_data(const char* input, const char* generics,const char* output)
{
	std::ifstream gens(generics);

	if (!gens)
	return;

	gentype gen;
	int32_t key;
	int32_t value;

	while (!gens.fail() && !gens.eof())
	{
		gens >> key >> value;
		gen.insert(gentype::value_type(key,value));
	}

	std::ifstream ifs(input);

	if (!ifs)
	return;
	int32_t shipping_service;
	int32_t origin_country;
	int32_t dest_country;
	int16_t min_hours;
	int16_t max_hours;
	cbt_map* bmap = new cbt_map();

	while (!ifs.fail() && !ifs.eof())
	{
		ifs >> shipping_service >> origin_country >> dest_country >> min_hours >> max_hours;
		shipping_service_info temp(min_hours,max_hours,(int8_t)0);
		cbt_key key(shipping_service,origin_country,dest_country);
		bmap->insert(std::pair<cbt_key, shipping_service_info>(key,temp));
		if (gen.count(shipping_service) > 0)
		{
			std::pair<gentype::iterator,gentype::iterator> range = gen.equal_range(shipping_service);
			for(gentype::iterator i = range.first; i!=range.second; i++)
			{
				cbt_key key2(i->second,origin_country,dest_country);
				bmap->insert(std::pair<cbt_key, shipping_service_info>(key2,temp));
			}
		}
	}

	std::ofstream ofs(output, std::ios_base::binary);
	boost::archive::binary_oarchive oarc(ofs);
	std::string out_text = output;
	out_text += ".txt";
	std::ofstream ofs_text(out_text.c_str());
	boost::archive::text_oarchive oarc_text(ofs_text);

	save_archive(oarc,*bmap);
	save_archive(oarc_text,*bmap);
	delete bmap;
	bmap=NULL;
}

/*
* Function to convert human readable file to Boost Serialization archive
* useful for unit testing 
*/
static void holiday_create_map_data(const char* input,const char* output)
{
	std::ifstream ifs(input);

	if (!ifs)
	return;
	
	/*
	* Get today's date, subtract 365 days, this will be our start day.
	*/
	int64_t start_date = static_cast<int64_t>(boost::gregorian::gregorian_calendar::day_number(
	boost::gregorian::day_clock::local_day_ymd()));
	start_date-=365;
	
	int32_t list_id;
	int32_t previous_list_id = -1;
	int16_t month;
	int16_t day;
	int16_t year;
	holiday_map* bmap = new holiday_map();
	holiday_info current(start_date);
	char comments[256];
	
	while (!ifs.fail() && !ifs.eof() && ifs.peek() == '#')
	{
		ifs.getline(comments, 256);
	}
	while (!ifs.fail() && !ifs.eof())
	{
		ifs >> list_id >> month >> day >> year;
		if (list_id != previous_list_id)
		{
			if (previous_list_id!=-1)
			{
				bmap->insert(std::pair<int32_t, holiday_info>(previous_list_id,current));
				current.holidays.reset();
			}
			previous_list_id=list_id;
		}
		
		current.set_bit(year,month,day);
	}
	bmap->insert(std::pair<int32_t, holiday_info>(previous_list_id, current));
	
	std::ofstream ofs(output, std::ios_base::binary);
	boost::archive::binary_oarchive oarc(ofs);
	std::string out_text = output;
	out_text += ".txt";
	std::ofstream ofs_text(out_text.c_str());
	boost::archive::text_oarchive oarc_text(ofs_text);

	save_archive(oarc,*bmap);
	save_archive(oarc_text,*bmap);
	delete bmap;
	bmap=NULL;
}

/* Analytical data holds 1 datum per day of the week, plus one for the total. */
static const int32_t analytical_info_data_size = 8;

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

	bool operator!=(const analytical_info &x) const
	{
		return (data[0]!=x.data[0] ||
		data[1]!=x.data[1] ||
		data[2]!=x.data[2] ||
		data[3]!=x.data[3] ||
		data[4]!=x.data[4] ||
		data[5]!=x.data[5] ||
		data[6]!=x.data[6] ||
		data[7]!=x.data[7]
		);
	}

	/** @brief Serialization function used by Boost serialization.
	*
	*  @param[in,out] ar The Archive to read/write to.
	*  @param[in] version Not used, but required by the interface.
	*/
	template <typename A>
	void serialize(A& ar, const unsigned int version)
	{
		ar & boost::serialization::make_array(data,analytical_info_data_size);
	}

	/* Array of raw data. */
	int16_t data[analytical_info_data_size];
};

/** @brief operator >> overload for reading shipping_zip_key
*/
std::istream &operator>>(std::istream &in, shipping_zip_key &s)
{
	in >> s.shipping_service_id >> s.origin_zip >> s.dest_zip;
	return in;
}

/** @brief The @a zip_key struct holds the lookup key for the shipping_zip
*    analytical map. It has a shipping method, a origin zip3 and a destination zip3.
*/
struct zip_key
{
	/** @brief Constructs a @a zip_key object.
	*    This is the default constructor.
	*/
	zip_key() :
	origin_zip(0),
	dest_zip(0)
	{
	}

	/** @brief Constructs a @a zip_key object.
	*
	*  @param[in] origin The origin zip.
	*  @param[in] dest The destination zip.
	*/
	zip_key(int16_t origin, int16_t dest) :
	origin_zip(origin),
	dest_zip(dest)
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

	bool operator!=(const zip_key& right) const
	{
		return !operator==(right);
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

	/** @brief Serialization function used by Boost serialization.
	*
	*  @param[in] ar The Archive to read/write to.
	*  @param[in] version Not used, but required by the interface.
	*/
	template<typename A>
	void serialize(A& ar, const unsigned int version)
	{
		ar & origin_zip;
		ar & dest_zip;
	}

	int16_t origin_zip;
	int16_t dest_zip;
};

/** @brief operator >> overload for reading zip_key
*/
std::istream &operator>>(std::istream &in, zip_key &z)
{
	in >> z.origin_zip >> z.dest_zip;
	return in;
}


/** @brief Define a hash_value function for zip_key. This is required for us
*    to use it as a key to a boost::unordered_map.
*
*  @param[in] key The instance of zip_key to hash.
*/
std::size_t hash_value(const zip_key& key)
{
	std::size_t hash = 0;
	boost::hash_combine(hash, 334213163 * key.origin_zip);
	boost::hash_combine(hash, 532999721 * key.dest_zip);
	return hash;
}

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


std::size_t hash_value(int32_t key)
{
	std::size_t hash = 0;
	boost::hash_combine(hash, key);
	return hash;
}

std::size_t hash_value(int64_t key)
{
	std::size_t hash = 0;
	boost::hash_combine(hash, key);
	return hash;
}

/** @brief A templated universal_hash function, with some large prime numbers.
*    Make sure to define the non-templatized universal_hash function for your
*    key type (or you'll get a segmentation fault).
*/
template <typename T, typename F>
std::size_t universal_hash_prime(const T& key, std::size_t a)
{
	std::size_t hash = F::universal_hash(key, a);

	if (a == 0)
	a = 179422921;
	hash = (a * 674505661 * hash);
	return hash;
}

bool bucket_sorter(std::pair<std::size_t,std::size_t> a, std::pair<std::size_t,std::size_t> b)
{
	return (a.second > b.second);
}

std::ostream &operator<<(std::ostream &in, shipping_zip_key &s)
{
	in << s.shipping_service_id <<" " << s.origin_zip<< " " << s.dest_zip;
	return in;
}



typedef ebay::common::perfect_hash_map<int64_t, analytical_info, int64_hasher> seller_map;
typedef boost::unordered_map<int64_t, analytical_info> category_map;
typedef boost::unordered_map<int32_t, analytical_info> shipping_map;
typedef ebay::common::perfect_hash_map<shipping_zip_key, analytical_info> shipping_zip_map;
typedef ebay::common::perfect_hash_map<zip_key, analytical_info> zip_map;

typedef uint16_t hash_t;

/*
* Function to convert human readable sellers data historical file to Boost Serialization archive
* useful for unittesting 
*/
template <typename M>
static void features_create_perfect_data(const char* input,const char* output)
{
	std::vector<typename M::value_type> vector;
	
	std::ifstream ifs(input);

	if (!ifs)
	{
		std::cout << "File Not Found: " << input << "\n";
		return;
	}
	std::cout << "Processing File: " << input << "\n";
	
	typename M::key_type input_id;
	typename M::mapped_type historical_features;
	
	ifs >> input_id;
	std::size_t count = 0;
	while (!ifs.fail() && !ifs.eof())
	{
		for(int i=0; i<analytical_info_data_size; ++i)
		{
			ifs >> historical_features.data[i];
		}
		typename M::value_type temp(input_id, historical_features);
		vector.push_back(temp);
		count++;
		ifs >> input_id;
	}

	std::cout << "Done reading: " << input << "\n";
	
	M* map = new M();
	if(map->create(vector,1.5,0.0005))
	{
		std::cout << "Created hash map with "<<map->get_bucket_count()<<" buckets: " << input << "\n";
	}
	else
	{
		std::cout << "Failed to Create PHM!!! " << input << "\n";
	}
	
	std::ofstream ofs(output, std::ios_base::binary);
	boost::archive::binary_oarchive oarc(ofs);
	std::string out_text = output;
	out_text += ".txt";
	std::ofstream ofs_text(out_text.c_str());
	boost::archive::text_oarchive oarc_text(ofs_text);

	oarc & *map;
	oarc_text & *map;
	delete map;
	map=NULL;
}

/*
* Function to convert human readable sellers data historical file to Boost Serialization archive
* useful for unittesting 
*/
template <typename T, typename M>
static void features_create_map_data(const char* input,const char* output)
{
	M* map = new M();
	
	std::ifstream ifs(input);

	if (!ifs)
	{
		std::cout << "File Not Found: " << input << "\n";
		return;
	}
	std::cout << "Processing File: " << input << "\n";
	
	T input_id;
	analytical_info historical_features;
	
	ifs >> input_id;
	int32_t count = 0;
	while (!ifs.fail() && !ifs.eof())
	{
		for(int i=0; i<analytical_info_data_size; ++i)
		{
			ifs >> historical_features.data[i];
		}
		map->insert(std::pair<T, analytical_info>(input_id, historical_features));
		count++;
		ifs >> input_id;
	}
	
	std::ofstream ofs(output, std::ios_base::binary);
	boost::archive::binary_oarchive oarc(ofs);
	std::string out_text = output;
	out_text += ".txt";
	std::ofstream ofs_text(out_text.c_str());
	boost::archive::text_oarchive oarc_text(ofs_text);

	save_archive(oarc,*map);
	save_archive(oarc_text,*map);
	delete map;
	map=NULL;
}

int main()
{

	ssi_create_map_data("shipping_services.txt","nde_shipping_service_info.dat");
	cbt_create_map_data("shipping_services_cbt.txt","generic_services.txt","nde_cbt_info.dat");
	holiday_create_map_data("holidays.txt","nde_shipping_service_holiday.dat");

	std::set<int16_t> excluded_zips = boost::assign::list_of(2898)(2899)(6798)(6799)(7151);
	zr_create_map_data("zip_ranges.txt","ade_zip_ranges.dat",excluded_zips);
	sb_create_map_data("base_services.txt","ade_base_services.dat");
	ze_create_map_data("zip_estimates.txt","ade_zip_estimates.dat");
	exc_create_map_data("exc_zones", "exc_zones.dat");
    z2zdefault_create_map_data("z2z_default", "z2z_default.dat");
    z2zranges_create_map_data("z2z_ranges", "z2z_ranges.dat");
    z2ztozipnull_create_map_data("z2z_tozipnull", "z2z_tozipnull.dat");
    z2z_create_map_data("z2z_ranges_data", "z2z_ranges_data.dat");
    z2z_services_create_map_data("z2z_services", "z2z_services.dat");
	
	features_create_map_data<int64_t, category_map>("category_history.txt","category_history.dat");
	features_create_map_data<int32_t, shipping_map>("shipment_history.txt","shipment_history.dat");

	features_create_perfect_data<zip_map>("zip_history.txt","zip_history.dat");
	features_create_perfect_data<seller_map>("seller_history.txt","seller_history.dat");
	features_create_perfect_data<shipping_zip_map>("shipment_zip_history.txt","shipment_zip_history.dat");
}